/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "presto_cpp/main/PrestoExchangeSource.h"

#include <fmt/core.h>
#include <folly/SocketAddress.h>
#include <re2/re2.h>
#include <sstream>

#include "presto_cpp/main/QueryContextManager.h"
#include "presto_cpp/main/common/Counters.h"
#include "presto_cpp/presto_protocol/presto_protocol.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/exec/Operator.h"

using namespace facebook::velox;

namespace facebook::presto {
namespace {

std::string extractTaskId(const std::string& path) {
  static const RE2 kPattern("/v1/task/([^/]*)/.*");
  std::string taskId;
  if (RE2::PartialMatch(path, kPattern, &taskId)) {
    return taskId;
  }

  VLOG(1) << "Failed to extract task ID from remote split: " << path;

  throw std::invalid_argument(
      fmt::format("Cannot extract task ID from remote split URL: {}", path));
}

void onFinalFailure(
    const std::string& errorMessage,
    std::shared_ptr<exec::ExchangeQueue> queue) {
  VLOG(1) << errorMessage;

  queue->setError(errorMessage);
}
} // namespace

PrestoExchangeSource::PrestoExchangeSource(
    const folly::Uri& baseUri,
    int destination,
    std::shared_ptr<exec::ExchangeQueue> queue,
    memory::MemoryPool* pool,
    const std::string& clientCertAndKeyPath,
    const std::string& ciphers)
    : ExchangeSource(extractTaskId(baseUri.path()), destination, queue, pool),
      basePath_(baseUri.path()),
      host_(baseUri.host()),
      port_(baseUri.port()),
      clientCertAndKeyPath_(clientCertAndKeyPath),
      ciphers_(ciphers) {
  folly::SocketAddress address(folly::IPAddress(host_).str(), port_, true);
  auto* eventBase = folly::getUnsafeMutableGlobalEventBase();
  httpClient_ = std::make_unique<http::HttpClient>(
      eventBase,
      address,
      std::chrono::milliseconds(10'000),
      clientCertAndKeyPath_,
      ciphers_,
      [](size_t bufferBytes) {
        REPORT_ADD_STAT_VALUE(kCounterHttpClientPrestoExchangeNumOnBody);
        REPORT_ADD_HISTOGRAM_VALUE(
            kCounterHttpClientPrestoExchangeOnBodyBytes, bufferBytes);
      });
}

bool PrestoExchangeSource::shouldRequestLocked() {
  if (atEnd_) {
    return false;
  }
  bool pending = requestPending_;
  requestPending_ = true;
  return !pending;
}

void PrestoExchangeSource::request() {
  failedAttempts_ = 0;
  doRequest();
}

void PrestoExchangeSource::doRequest() {
  if (closed_.load()) {
    queue_->setError("PrestoExchangeSource closed");
    return;
  }
  auto path = fmt::format("{}/{}", basePath_, sequence_);
  VLOG(1) << "Fetching data from " << host_ << ":" << port_ << " " << path;
  auto self = getSelfPtr();
  http::RequestBuilder()
      .method(proxygen::HTTPMethod::GET)
      .url(path)
      .header(protocol::PRESTO_MAX_SIZE_HTTP_HEADER, "32MB")
      .send(httpClient_.get(), pool_.get())
      .via(driverCPUExecutor())
      .thenValue([path, self](std::unique_ptr<http::HttpResponse> response) {
        velox::common::testutil::TestValue::adjust(
            "facebook::presto::PrestoExchangeSource::doRequest", self.get());
        auto* headers = response->headers();
        if (headers->getStatusCode() != http::kHttpOk &&
            headers->getStatusCode() != http::kHttpNoContent) {
          self->processDataError(
              path,
              fmt::format(
                  "Received HTTP {} {}",
                  headers->getStatusCode(),
                  headers->getStatusMessage()));
        } else if (response->hasError()) {
          self->processDataError(path, response->error(), false);
        } else {
          self->processDataResponse(std::move(response));
        }
      })
      .thenError(
          folly::tag_t<std::exception>{},
          [path, self](const std::exception& e) {
            self->processDataError(path, e.what());
          });
};

void PrestoExchangeSource::processDataResponse(
    std::unique_ptr<http::HttpResponse> response) {
  if (closed_.load()) {
    // If PrestoExchangeSource is already closed, just free all buffers
    // allocated without doing any processing. This can happen when a super slow
    // response comes back after its owning 'Task' gets destroyed.
    response->freeBuffers();
    return;
  }
  auto* headers = response->headers();
  VELOX_CHECK(
      !headers->getIsChunked(),
      "Chunked http transferring encoding is not supported.")
  uint64_t contentLength =
      atol(headers->getHeaders()
               .getSingleOrEmpty(proxygen::HTTP_HEADER_CONTENT_LENGTH)
               .c_str());
  VLOG(1) << "Fetched data for " << basePath_ << "/" << sequence_ << ": "
          << contentLength << " bytes";

  auto complete = headers->getHeaders()
                      .getSingleOrEmpty(protocol::PRESTO_BUFFER_COMPLETE_HEADER)
                      .compare("true") == 0;
  if (complete) {
    VLOG(1) << "Received buffer-complete header for " << basePath_ << "/"
            << sequence_;
  }

  int64_t ackSequence =
      atol(headers->getHeaders()
               .getSingleOrEmpty(protocol::PRESTO_PAGE_NEXT_TOKEN_HEADER)
               .c_str());

  std::unique_ptr<exec::SerializedPage> page;
  std::unique_ptr<folly::IOBuf> singleChain;
  const bool empty = response->empty();
  int64_t totalBytes{0};
  if (!empty) {
    auto iobufs = response->consumeBody();
    for (auto& buf : iobufs) {
      totalBytes += buf->capacity();
      if (!singleChain) {
        singleChain = std::move(buf);
      } else {
        singleChain->prev()->appendChain(std::move(buf));
      }
    }
    PrestoExchangeSource::updateMemoryUsage(totalBytes);

    page = std::make_unique<exec::SerializedPage>(
        std::move(singleChain), [pool = pool_](folly::IOBuf& iobuf) {
          int64_t freedBytes{0};
          // Free the backed memory from MemoryAllocator on page dtor
          folly::IOBuf* start = &iobuf;
          auto curr = start;
          do {
            freedBytes += curr->capacity();
            pool->free(curr->writableData(), curr->capacity());
            curr = curr->next();
          } while (curr != start);
          PrestoExchangeSource::updateMemoryUsage(-freedBytes);
        });
  }

  REPORT_ADD_HISTOGRAM_VALUE(
      kCounterPrestoExchangeSerializedPageSize, page ? page->size() : 0);

  {
    std::vector<ContinuePromise> promises;
    {
      std::lock_guard<std::mutex> l(queue_->mutex());
      if (page) {
        VLOG(1) << "Enqueuing page for " << basePath_ << "/" << sequence_
                << ": " << page->size() << " bytes";
        ++numPages_;
        queue_->enqueueLocked(std::move(page), promises);
      }
      if (complete) {
        VLOG(1) << "Enqueuing empty page for " << basePath_ << "/" << sequence_;
        atEnd_ = true;
        queue_->enqueueLocked(nullptr, promises);
      }

      sequence_ = ackSequence;

      // Reset requestPending_ if the response is complete or have pages.
      if (complete || !empty) {
        requestPending_ = false;
      }
    }
    for (auto& promise : promises) {
      promise.setValue();
    }
  }

  if (complete) {
    abortResults();
  } else {
    if (!empty) {
      // Acknowledge results for non-empty content.
      acknowledgeResults(ackSequence);
    } else {
      // Rerequest results for incomplete results with no pages.
      request();
    }
  }
}

void PrestoExchangeSource::processDataError(
    const std::string& path,
    const std::string& error,
    bool retry) {
  ++failedAttempts_;
  if (retry && failedAttempts_ < 3) {
    VLOG(1) << "Failed to fetch data from " << host_ << ":" << port_ << " "
            << path << " - Retrying: " << error;

    doRequest();
    return;
  }

  onFinalFailure(
      fmt::format(
          "Failed to fetched data from {}:{} {} - Exhausted retries: {}",
          host_,
          port_,
          path,
          error),
      queue_);
}

void PrestoExchangeSource::acknowledgeResults(int64_t ackSequence) {
  auto ackPath = fmt::format("{}/{}/acknowledge", basePath_, ackSequence);
  VLOG(1) << "Sending ack " << ackPath;
  auto self = getSelfPtr();

  http::RequestBuilder()
      .method(proxygen::HTTPMethod::GET)
      .url(ackPath)
      .send(httpClient_.get(), pool_.get())
      .via(driverCPUExecutor())
      .thenValue([self](std::unique_ptr<http::HttpResponse> response) {
        VLOG(1) << "Ack " << response->headers()->getStatusCode();
      })
      .thenError(
          folly::tag_t<std::exception>{}, [self](const std::exception& e) {
            // Acks are optional. No need to fail the query.
            VLOG(1) << "Ack failed: " << e.what();
          });
}

void PrestoExchangeSource::abortResults() {
  VLOG(1) << "Sending abort results " << basePath_;
  auto queue = queue_;
  auto self = getSelfPtr();
  http::RequestBuilder()
      .method(proxygen::HTTPMethod::DELETE)
      .url(basePath_)
      .send(httpClient_.get(), pool_.get())
      .via(driverCPUExecutor())
      .thenValue([queue, self](std::unique_ptr<http::HttpResponse> response) {
        auto statusCode = response->headers()->getStatusCode();
        if (statusCode != http::kHttpOk && statusCode != http::kHttpNoContent) {
          const std::string errMsg = fmt::format(
              "Abort results failed: {}, path {}", statusCode, self->basePath_);
          LOG(ERROR) << errMsg;
          onFinalFailure(errMsg, queue);
        } else {
          self->abortResultsSucceeded_.store(true);
        }
      })
      .thenError(
          folly::tag_t<std::exception>{},
          [queue, self](const std::exception& e) {
            const std::string errMsg = fmt::format(
                "Abort results failed: {}, path {}", e.what(), self->basePath_);
            LOG(ERROR) << errMsg;
            // Captures 'queue' by value to ensure lifetime. Error
            // detection can be arbitrarily late, for example after cancellation
            // due to other errors.
            onFinalFailure(errMsg, queue);
          });
}

void PrestoExchangeSource::close() {
  closed_.store(true);
  if (!abortResultsSucceeded_.load()) {
    abortResults();
  }
}

std::shared_ptr<PrestoExchangeSource> PrestoExchangeSource::getSelfPtr() {
  return std::dynamic_pointer_cast<PrestoExchangeSource>(shared_from_this());
}

// static
std::unique_ptr<exec::ExchangeSource>
PrestoExchangeSource::createExchangeSource(
    const std::string& url,
    int destination,
    std::shared_ptr<exec::ExchangeQueue> queue,
    memory::MemoryPool* pool) {
  if (strncmp(url.c_str(), "http://", 7) == 0) {
    return std::make_unique<PrestoExchangeSource>(
        folly::Uri(url), destination, queue, pool);
  } else if (strncmp(url.c_str(), "https://", 8) == 0) {
    const auto systemConfig = SystemConfig::instance();
    const auto clientCertAndKeyPath =
        systemConfig->httpsClientCertAndKeyPath().value_or("");
    const auto ciphers = systemConfig->httpsSupportedCiphers();
    return std::make_unique<PrestoExchangeSource>(
        folly::Uri(url),
        destination,
        queue,
        pool,
        clientCertAndKeyPath,
        ciphers);
  }
  return nullptr;
}

void PrestoExchangeSource::updateMemoryUsage(int64_t updateBytes) {
  const int64_t newMemoryBytes =
      currQueuedMemoryBytes().fetch_add(updateBytes) + updateBytes;
  if (updateBytes > 0) {
    peakQueuedMemoryBytes() =
        std::max<int64_t>(peakQueuedMemoryBytes(), newMemoryBytes);
  } else {
    VELOX_CHECK_GE(currQueuedMemoryBytes(), 0);
  }
}

void PrestoExchangeSource::getMemoryUsage(
    int64_t& currentBytes,
    int64_t& peakBytes) {
  currentBytes = currQueuedMemoryBytes();
  peakBytes = peakQueuedMemoryBytes();
}

void PrestoExchangeSource::testingClearMemoryUsage() {
  currQueuedMemoryBytes() = 0;
  peakQueuedMemoryBytes() = 0;
}
} // namespace facebook::presto
