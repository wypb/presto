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
#include <folly/executors/ThreadedExecutor.h>
#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include <velox/common/memory/MemoryAllocator.h>
#include "presto_cpp/main/PrestoExchangeSource.h"
#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/http/HttpClient.h"
#include "presto_cpp/main/http/HttpServer.h"
#include "presto_cpp/main/tests/HttpServerWrapper.h"
#include "presto_cpp/presto_protocol/presto_protocol.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/memory/MmapAllocator.h"
#include "velox/common/testutil/TestValue.h"

using namespace facebook::presto;
using namespace facebook::velox;
using namespace facebook::velox::memory;
using namespace facebook::velox::common::testutil;

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  return RUN_ALL_TESTS();
}

class Producer {
 public:
  void registerEndpoints(http::HttpServer* server) {
    server->registerGet(
        R"(/v1/task/(.*)/results/([0-9]+)/([0-9]+))",
        [this](
            proxygen::HTTPMessage* message,
            const std::vector<std::string>& pathMatch) {
          return getResults(message, pathMatch);
        });
    server->registerGet(
        R"(/v1/task/(.+)/results/([0-9]+)/([0-9]+)/acknowledge)",
        [this](
            proxygen::HTTPMessage* message,
            const std::vector<std::string>& pathMatch) {
          return acknowledgeResults(message, pathMatch);
        });
    server->registerDelete(
        R"(/v1/task/(.+)/results/([0-9]+))",
        [this](
            proxygen::HTTPMessage* message,
            const std::vector<std::string>& pathMatch) {
          return deleteResults(message, pathMatch);
        });
  }

  proxygen::RequestHandler* getResults(
      proxygen::HTTPMessage* /*message*/,
      const std::vector<std::string>& pathMatch) {
    protocol::TaskId taskId = pathMatch[1];
    long sequence = std::stol(pathMatch[3]);

    return new http::CallbackRequestHandler(
        [this, taskId, sequence](
            proxygen::HTTPMessage* /*message*/,
            const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
            proxygen::ResponseHandler* downstream) {
          auto [data, noMoreData] = getData(sequence);
          if (!data.empty() || noMoreData) {
            sendResponse(downstream, taskId, sequence, data, noMoreData);
          } else {
            auto [promise, future] = folly::makePromiseContract<bool>();

            std::move(future)
                .via(folly::EventBaseManager::get()->getEventBase())
                .thenValue([this, downstream, taskId, sequence](
                               bool /*value*/) {
                  auto [data, noMoreData] = getData(sequence);
                  VELOX_CHECK(!data.empty() || noMoreData);
                  sendResponse(downstream, taskId, sequence, data, noMoreData);
                });

            promise_ = std::move(promise);
          }
        });
  }

  proxygen::RequestHandler* acknowledgeResults(
      proxygen::HTTPMessage* /*message*/,
      const std::vector<std::string>& pathMatch) {
    long sequence = std::stol(pathMatch[3]);
    return new http::CallbackRequestHandler(
        [this, sequence](
            proxygen::HTTPMessage* /*message*/,
            const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
            proxygen::ResponseHandler* downstream) {
          auto lastAckPromise = folly::Promise<bool>::makeEmpty();
          {
            std::lock_guard<std::mutex> l(mutex_);
            if (sequence > startSequence_) {
              for (int i = startSequence_; i < sequence && !queue_.empty();
                   ++i) {
                queue_.pop_front();
              }
              startSequence_ = sequence;
            }

            if (queue_.empty() && noMoreData_) {
              lastAckPromise = std::move(deleteResultsPromise_);
            }
          }

          proxygen::ResponseBuilder(downstream)
              .status(http::kHttpOk, "OK")
              .sendWithEOM();

          if (lastAckPromise.valid()) {
            lastAckPromise.setValue(true);
          }
        });
  }

  proxygen::RequestHandler* deleteResults(
      proxygen::HTTPMessage* /*message*/,
      const std::vector<std::string>& /*pathMatch*/) {
    return new http::CallbackRequestHandler(
        [this](
            proxygen::HTTPMessage* /*message*/,
            const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
            proxygen::ResponseHandler* downstream) {
          auto deleteResultsPromise = folly::Promise<bool>::makeEmpty();
          {
            std::lock_guard<std::mutex> l(mutex_);
            queue_.clear();
            receivedDeleteResults_ = true;
            deleteResultsPromise = std::move(deleteResultsPromise_);
          }

          proxygen::ResponseBuilder(downstream)
              .status(http::kHttpOk, "OK")
              .sendWithEOM();

          if (deleteResultsPromise.valid()) {
            deleteResultsPromise.setValue(true);
          }
        });
  }

  void enqueue(const std::string& data) {
    auto promise = folly::Promise<bool>::makeEmpty();
    {
      std::lock_guard<std::mutex> l(mutex_);
      queue_.emplace_back(data);

      if (promise_.valid()) {
        promise = std::move(promise_);
      }
    }

    if (promise.valid()) {
      promise.setValue(true);
    }
  }

  void noMoreData() {
    std::lock_guard<std::mutex> l(mutex_);
    noMoreData_ = true;
  }

  void waitForDeleteResults() {
    folly::SemiFuture<bool> future(false);
    {
      std::lock_guard<std::mutex> l(mutex_);
      if (receivedDeleteResults_) {
        return;
      }

      auto [p, f] = folly::makePromiseContract<bool>();
      deleteResultsPromise_ = std::move(p);
      future = std::move(f);
    }

    std::move(future).get(std::chrono::microseconds(120'000));
  }

  folly::Promise<bool>& promise() {
    return promise_;
  }

 private:
  std::tuple<std::string, bool> getData(int64_t sequence) {
    std::string data;
    bool noMoreData = false;
    {
      std::lock_guard<std::mutex> l(mutex_);
      auto index = sequence - startSequence_;
      VELOX_CHECK_GE(index, 0);
      if (queue_.size() > index) {
        data = queue_[index];
      } else {
        noMoreData = noMoreData_;
      }
    }
    return std::make_tuple(std::move(data), noMoreData);
  }

  void sendResponse(
      proxygen::ResponseHandler* downstream,
      const protocol::TaskId& taskId,
      int64_t sequence,
      const std::string& data,
      bool complete) {
    proxygen::ResponseBuilder builder(downstream);
    builder.status(http::kHttpOk, "OK")
        .header(protocol::PRESTO_TASK_INSTANCE_ID_HEADER, taskId)
        .header(protocol::PRESTO_PAGE_TOKEN_HEADER, std::to_string(sequence))
        .header(
            protocol::PRESTO_PAGE_NEXT_TOKEN_HEADER,
            std::to_string(sequence + 1))
        .header(
            protocol::PRESTO_BUFFER_COMPLETE_HEADER,
            complete ? "true" : "false");
    if (!data.empty()) {
      auto buffer = folly::IOBuf::create(4 + data.size());
      int32_t dataSize = data.size();
      memcpy(
          buffer->writableData(), reinterpret_cast<const char*>(&dataSize), 4);
      memcpy(buffer->writableData() + 4, data.data(), dataSize);
      buffer->append(4 + dataSize);
      builder
          .header(
              proxygen::HTTP_HEADER_CONTENT_TYPE,
              protocol::PRESTO_PAGES_MIME_TYPE)
          .body(std::move(buffer));
    }
    builder.sendWithEOM();
  }

  std::deque<std::string> queue_;
  std::mutex mutex_;
  bool noMoreData_ = false;
  int startSequence_ = 0;
  folly::Promise<bool> promise_ = folly::Promise<bool>::makeEmpty();
  folly::Promise<bool> deleteResultsPromise_ =
      folly::Promise<bool>::makeEmpty();
  bool receivedDeleteResults_ = false;
};

std::string toString(exec::SerializedPage* page) {
  ByteStream input;
  page->prepareStreamForDeserialize(&input);

  auto numBytes = input.read<int32_t>();
  char data[numBytes + 1];
  input.readBytes(data, numBytes);
  data[numBytes] = '\0';
  return std::string(data);
}

std::unique_ptr<exec::SerializedPage> waitForNextPage(
    const std::shared_ptr<exec::ExchangeQueue>& queue) {
  bool atEnd;
  facebook::velox::ContinueFuture future;
  auto page = queue->dequeueLocked(&atEnd, &future);
  EXPECT_FALSE(atEnd);
  if (page == nullptr) {
    std::move(future).get();
    page = queue->dequeueLocked(&atEnd, &future);
    EXPECT_TRUE(page != nullptr);
  }
  return page;
}

void waitForEndMarker(const std::shared_ptr<exec::ExchangeQueue>& queue) {
  bool atEnd;
  facebook::velox::ContinueFuture future;
  auto page = queue->dequeueLocked(&atEnd, &future);
  ASSERT_TRUE(page == nullptr);
  if (!atEnd) {
    std::move(future).get();
    page = queue->dequeueLocked(&atEnd, &future);
    ASSERT_TRUE(page == nullptr);
    ASSERT_TRUE(atEnd);
  }
}

folly::Uri makeProducerUri(const folly::SocketAddress& address) {
  return folly::Uri(fmt::format(
      "http://{}:{}/v1/task/20201007_190402_00000_r5erw.1.0.0/results/3",
      address.getAddressStr(),
      address.getPort()));
}

class PrestoExchangeSourceTest : public testing::Test {
 public:
  void SetUp() override {
    auto& defaultManager = memory::MemoryManager::getInstance();
    pool_ = memory::addDefaultLeafMemoryPool("PrestoExchangeSourceTest");
    memory::MmapAllocator::Options options;
    options.capacity = 1L << 30;
    allocator_ = std::make_unique<memory::MmapAllocator>(options);
    memory::MemoryAllocator::setDefaultInstance(allocator_.get());
    TestValue::enable();
  }

  void TearDown() override {
    memory::MemoryAllocator::setDefaultInstance(nullptr);
    TestValue::disable();
  }

  void requestNextPage(
      const std::shared_ptr<exec::ExchangeQueue>& queue,
      const std::shared_ptr<exec::ExchangeSource>& exchangeSource) {
    {
      std::lock_guard<std::mutex> l(queue->mutex());
      ASSERT_TRUE(exchangeSource->shouldRequestLocked());
    }
    exchangeSource->request();
  }

  std::shared_ptr<memory::MemoryPool> pool_;
  std::unique_ptr<memory::MemoryAllocator> allocator_;
};

TEST_F(PrestoExchangeSourceTest, basic) {
  std::vector<std::string> pages = {"page1 - xx", "page2 - xxxxx"};
  auto producer = std::make_unique<Producer>();
  for (auto& page : pages) {
    producer->enqueue(page);
  }
  producer->noMoreData();

  auto producerServer = std::make_unique<http::HttpServer>(
      std::make_unique<http::HttpConfig>(folly::SocketAddress("127.0.0.1", 0)));
  producer->registerEndpoints(producerServer.get());

  test::HttpServerWrapper serverWrapper(std::move(producerServer));
  auto producerAddress = serverWrapper.start().get();
  auto producerUri = makeProducerUri(producerAddress);

  auto queue = std::make_shared<exec::ExchangeQueue>(1 << 20);
  queue->addSourceLocked();
  queue->noMoreSources();

  auto exchangeSource = std::make_shared<PrestoExchangeSource>(
      producerUri, 3, queue, pool_.get());

  size_t beforePoolSize = pool_->getCurrentBytes();
  size_t beforeQueueSize = queue->totalBytes();
  requestNextPage(queue, exchangeSource);
  for (int i = 0; i < pages.size(); i++) {
    auto page = waitForNextPage(queue);
    ASSERT_EQ(toString(page.get()), pages[i]) << "at " << i;
    requestNextPage(queue, exchangeSource);
  }
  waitForEndMarker(queue);

  size_t deltaPool = pool_->getCurrentBytes() - beforePoolSize;
  size_t deltaQueue = queue->totalBytes() - beforeQueueSize;
  EXPECT_EQ(deltaPool, deltaQueue);

  producer->waitForDeleteResults();
  serverWrapper.stop();
  EXPECT_EQ(pool_->getCurrentBytes(), 0);

  const auto stats = exchangeSource->stats();
  ASSERT_EQ(stats.size(), 1);
  const auto it = stats.find("prestoExchangeSource.numPages");
  ASSERT_EQ(it->second, 2);
}

TEST_F(PrestoExchangeSourceTest, earlyTerminatingConsumer) {
  std::vector<std::string> pages = {"page1 - xx", "page2 - xxxxx"};
  auto producer = std::make_unique<Producer>();
  for (auto& page : pages) {
    producer->enqueue(page);
  }
  producer->noMoreData();

  auto producerServer = std::make_unique<http::HttpServer>(
      std::make_unique<http::HttpConfig>(folly::SocketAddress("127.0.0.1", 0)));
  producer->registerEndpoints(producerServer.get());

  test::HttpServerWrapper serverWrapper(std::move(producerServer));
  auto producerAddress = serverWrapper.start().get();
  auto producerUri = makeProducerUri(producerAddress);

  auto queue = std::make_shared<exec::ExchangeQueue>(1 << 20);
  queue->addSourceLocked();
  queue->noMoreSources();

  auto exchangeSource = std::make_shared<PrestoExchangeSource>(
      makeProducerUri(producerAddress), 3, queue, pool_.get());
  exchangeSource->close();

  producer->waitForDeleteResults();
  serverWrapper.stop();
  EXPECT_EQ(pool_->getCurrentBytes(), 0);

  const auto stats = exchangeSource->stats();
  ASSERT_EQ(stats.size(), 1);
  const auto it = stats.find("prestoExchangeSource.numPages");
  ASSERT_EQ(it->second, 0);
}

TEST_F(PrestoExchangeSourceTest, slowProducer) {
  std::vector<std::string> pages = {"page1 - xx", "page2 - xxxxx"};
  auto producer = std::make_unique<Producer>();

  auto producerServer = std::make_unique<http::HttpServer>(
      std::make_unique<http::HttpConfig>(folly::SocketAddress("127.0.0.1", 0)));
  producer->registerEndpoints(producerServer.get());

  test::HttpServerWrapper serverWrapper(std::move(producerServer));
  auto producerAddress = serverWrapper.start().get();

  auto queue = std::make_shared<exec::ExchangeQueue>(1 << 20);
  queue->addSourceLocked();
  queue->noMoreSources();
  auto exchangeSource = std::make_shared<PrestoExchangeSource>(
      makeProducerUri(producerAddress), 3, queue, pool_.get());

  size_t beforePoolSize = pool_->getCurrentBytes();
  size_t beforeQueueSize = queue->totalBytes();
  requestNextPage(queue, exchangeSource);
  for (int i = 0; i < pages.size(); i++) {
    producer->enqueue(pages[i]);
    auto page = waitForNextPage(queue);
    ASSERT_EQ(toString(page.get()), pages[i]) << "at " << i;
    requestNextPage(queue, exchangeSource);
  }
  producer->noMoreData();
  waitForEndMarker(queue);

  size_t deltaPool = pool_->getCurrentBytes() - beforePoolSize;
  size_t deltaQueue = queue->totalBytes() - beforeQueueSize;
  EXPECT_EQ(deltaPool, deltaQueue);

  producer->waitForDeleteResults();
  serverWrapper.stop();
  EXPECT_EQ(pool_->getCurrentBytes(), 0);

  const auto stats = exchangeSource->stats();
  ASSERT_EQ(stats.size(), 1);
  const auto it = stats.find("prestoExchangeSource.numPages");
  ASSERT_EQ(it->second, pages.size());
}

TEST_F(PrestoExchangeSourceTest, slowProducerAndEarlyTerminatingConsumer) {
  std::atomic<bool> codePointHit{false};
  SCOPED_TESTVALUE_SET(
      "facebook::presto::PrestoExchangeSource::doRequest",
      std::function<void(const PrestoExchangeSource*)>(
          ([&](const auto* prestoExchangeSource) { codePointHit = true; })));
  auto producer = std::make_unique<Producer>();

  auto producerServer = std::make_unique<http::HttpServer>(
      std::make_unique<http::HttpConfig>(folly::SocketAddress("127.0.0.1", 0)));
  producer->registerEndpoints(producerServer.get());

  test::HttpServerWrapper serverWrapper(std::move(producerServer));
  auto producerAddress = serverWrapper.start().get();

  auto queue = std::make_shared<exec::ExchangeQueue>(1 << 20);
  queue->addSourceLocked();
  queue->noMoreSources();
  auto exchangeSource = std::make_shared<PrestoExchangeSource>(
      makeProducerUri(producerAddress), 3, queue, pool_.get());

  requestNextPage(queue, exchangeSource);

  // Simulation of an early destruction of 'Task' will release following
  // resources, including pool_
  exchangeSource->close();
  queue->close();
  exchangeSource.reset();
  queue.reset();
  pool_.reset();

  // We want to wait a bit on the promise state to be valid. That way we are
  // sure getResults() on the server side (producer) goes into empty data
  // condition because we did not enqueue any data in producer yet. This allows
  // us to have full control to simulate a super late response return by
  // enqueuing a result afterwards.
  while (!producer->promise().valid()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }

  // We shall not crash here when response comes back super late.
  producer->enqueue("I'm a super slow response");

  // We need to wait a bit for response handling mechanism to happen in the
  // background. There is no way to know where we are for response handling as
  // all resources have been cleaned up, so explicitly waiting is the only way
  // to allow the execution of background processing. We expect the test to not
  // crash.
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  EXPECT_TRUE(codePointHit);
  serverWrapper.stop();
}

TEST_F(PrestoExchangeSourceTest, failedProducer) {
  std::vector<std::string> pages = {"page1 - xx", "page2 - xxxxx"};
  auto producer = std::make_unique<Producer>();

  auto producerServer = std::make_unique<http::HttpServer>(
      std::make_unique<http::HttpConfig>(folly::SocketAddress("127.0.0.1", 0)));
  producer->registerEndpoints(producerServer.get());

  test::HttpServerWrapper serverWrapper(std::move(producerServer));
  auto producerAddress = serverWrapper.start().get();

  auto queue = std::make_shared<exec::ExchangeQueue>(1 << 20);
  queue->addSourceLocked();
  queue->noMoreSources();
  auto exchangeSource = std::make_shared<PrestoExchangeSource>(
      makeProducerUri(producerAddress), 3, queue, pool_.get());

  requestNextPage(queue, exchangeSource);
  producer->enqueue(pages[0]);

  // Stop server to simulate failed connection.
  serverWrapper.stop();

  EXPECT_THROW(waitForNextPage(queue), std::runtime_error);
}

TEST_F(PrestoExchangeSourceTest, exceedingMemoryCapacityForHttpResponse) {
  const int64_t memoryCapBytes = 1 << 10;
  auto rootPool = defaultMemoryManager().addRootPool(
      "httpResponseAllocationFailure", memoryCapBytes);
  auto leafPool =
      rootPool->addLeafChild("exceedingMemoryCapacityForHttpResponse");

  auto producer = std::make_unique<Producer>();

  auto producerServer = std::make_unique<http::HttpServer>(
      std::make_unique<http::HttpConfig>(folly::SocketAddress("127.0.0.1", 0)));
  producer->registerEndpoints(producerServer.get());

  test::HttpServerWrapper serverWrapper(std::move(producerServer));
  auto producerAddress = serverWrapper.start().get();

  auto queue = std::make_shared<exec::ExchangeQueue>(1 << 20);
  queue->addSourceLocked();
  queue->noMoreSources();
  auto exchangeSource = std::make_shared<PrestoExchangeSource>(
      makeProducerUri(producerAddress), 3, queue, leafPool.get());

  requestNextPage(queue, exchangeSource);
  const std::string largePayload(2 * memoryCapBytes, 'L');

  producer->enqueue(largePayload);
  ASSERT_ANY_THROW(waitForNextPage(queue));
  producer->noMoreData();
  // Verify that we never retry on memory allocation failure of the http
  // response data but just fails the query.
  ASSERT_EQ(exchangeSource->testingFailedAttempts(), 1);
  ASSERT_EQ(leafPool->getCurrentBytes(), 0);
}

TEST_F(PrestoExchangeSourceTest, memoryAllocationAndUsageCheck) {
  auto rootPool =
      defaultMemoryManager().addRootPool("memoryAllocationAndUsageCheck");
  auto leafPool = rootPool->addLeafChild("memoryAllocationAndUsageCheck");

  auto producer = std::make_unique<Producer>();

  auto producerServer = std::make_unique<http::HttpServer>(
      std::make_unique<http::HttpConfig>(folly::SocketAddress("127.0.0.1", 0)));
  producer->registerEndpoints(producerServer.get());

  test::HttpServerWrapper serverWrapper(std::move(producerServer));
  auto producerAddress = serverWrapper.start().get();

  auto queue = std::make_shared<exec::ExchangeQueue>(1 << 20);
  queue->addSourceLocked();
  queue->noMoreSources();
  auto exchangeSource = std::make_shared<PrestoExchangeSource>(
      makeProducerUri(producerAddress), 3, queue, leafPool.get());

  const std::string smallPayload(7 << 10, 'L');
  producer->enqueue(smallPayload);
  requestNextPage(queue, exchangeSource);
  auto smallPage = waitForNextPage(queue);
  ASSERT_EQ(leafPool->stats().numAllocs, 2);
  int64_t currMemoryBytes;
  int64_t peakMemoryBytes;
  PrestoExchangeSource::getMemoryUsage(currMemoryBytes, peakMemoryBytes);
  ASSERT_EQ(
      memory::AllocationTraits::pageBytes(pool_->sizeClasses().front()) *
          (1 + 2),
      currMemoryBytes);
  ASSERT_EQ(
      memory::AllocationTraits::pageBytes(pool_->sizeClasses().front()) *
          (1 + 2),
      peakMemoryBytes);
  smallPage.reset();
  PrestoExchangeSource::getMemoryUsage(currMemoryBytes, peakMemoryBytes);
  ASSERT_EQ(0, currMemoryBytes);
  ASSERT_EQ(
      memory::AllocationTraits::pageBytes(pool_->sizeClasses().front()) *
          (1 + 2),
      peakMemoryBytes);

  const std::string largePayload(128 << 10, 'L');
  producer->enqueue(largePayload);
  requestNextPage(queue, exchangeSource);
  auto largePage = waitForNextPage(queue);
  producer->noMoreData();

  PrestoExchangeSource::getMemoryUsage(currMemoryBytes, peakMemoryBytes);

  ASSERT_EQ(
      memory::AllocationTraits::pageBytes(pool_->sizeClasses().front()) *
          (1 + 2 + 4 + 8 + 16 + 16),
      currMemoryBytes);
  ASSERT_EQ(
      memory::AllocationTraits::pageBytes(pool_->sizeClasses().front()) *
          (1 + 2 + 4 + 8 + 16 + 16),
      peakMemoryBytes);
  largePage.reset();
  PrestoExchangeSource::getMemoryUsage(currMemoryBytes, peakMemoryBytes);
  ASSERT_EQ(0, currMemoryBytes);
  ASSERT_EQ(
      memory::AllocationTraits::pageBytes(pool_->sizeClasses().front()) *
          (1 + 2 + 4 + 8 + 16 + 16),
      peakMemoryBytes);

  requestNextPage(queue, exchangeSource);
  waitForEndMarker(queue);
  serverWrapper.stop();
  PrestoExchangeSource::getMemoryUsage(currMemoryBytes, peakMemoryBytes);
  ASSERT_EQ(0, currMemoryBytes);
  ASSERT_EQ(192512, peakMemoryBytes);
}
