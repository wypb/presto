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
#include "presto_cpp/main/operators/TestingPersistentShuffle.h"

using namespace facebook::velox::exec;
using namespace facebook::velox;

namespace facebook::presto::operators {

namespace {
inline std::string createShuffleFileName(
    const std::string& rootPath,
    int32_t partition,
    int fileIndex,
    const std::thread::id& id) {
  return fmt::format("{}/{}_{}_{}.bin", rootPath, partition, fileIndex, id);
}

// This file is used to indicate that the shuffle system is ready to be used for
// reading (acts as a sync point between readers if needed). Mostly used for
// test purposes.
const static std::string kReadyForReadFilename = "readyForRead";
}; // namespace

void TestingPersistentShuffle::initialize(
    velox::memory::MemoryPool* pool,
    uint32_t numPartitions,
    std::string rootPath) {
  pool_ = pool;
  numPartitions_ = numPartitions;
  // Use resize/assign instead of resize(size, val).
  inProgressPartitions_.resize(numPartitions_);
  inProgressPartitions_.assign(numPartitions_, nullptr);
  inProgressSizes_.resize(numPartitions_);
  inProgressSizes_.assign(numPartitions_, 0);
  readPartitionsFileIndex_.resize(numPartitions_);
  readPartitionsFileIndex_.assign(numPartitions_, 0);
  readPartitionFiles_.resize(numPartitions_);
  readPartitionFiles_.assign(numPartitions_, {});
  if (rootPath_ != "") {
    cleanup();
  }
  rootPath_ = std::move(rootPath);
  fileSystem_ = velox::filesystems::getFileSystem(rootPath_, nullptr);
}

std::unique_ptr<velox::WriteFile> TestingPersistentShuffle::getNextOutputFile(
    int32_t partition) {
  auto fileCount = getWritePartitionFilesCount(partition);
  const std::string filename =
      createShuffleFileName(rootPath_, partition, fileCount, threadId_);
  return fileSystem_->openFileForWrite(filename);
}

int TestingPersistentShuffle::getWritePartitionFilesCount(
    int32_t partition) const {
  int fileCount = 0;
  // TODO: consider to maintain the next to create file count in memory as we
  // always do cleanup when switch to a new root directy path.
  do {
    const std::string filename =
        createShuffleFileName(rootPath_, partition, fileCount, threadId_);
    if (!fileSystem_->exists(filename)) {
      break;
    }
    ++fileCount;
  } while (true);
  return fileCount;
}

std::vector<std::string> TestingPersistentShuffle::getReadPartitionFiles(
    int32_t partition) const {
  // Get rid of excess '/' characters in the path.
  auto trimmedRootPath = rootPath_;
  while (trimmedRootPath.length() > 0 &&
         trimmedRootPath[trimmedRootPath.length() - 1] == '/') {
    trimmedRootPath.erase(trimmedRootPath.length() - 1, 1);
  }

  std::string partitionFilePrefix =
      fmt::format("{}/{}_", trimmedRootPath, partition);
  std::vector<std::string> partitionFiles;
  auto files = fileSystem_->list(rootPath_);
  for (auto& file : files) {
    if (file.find(partitionFilePrefix) == 0) {
      partitionFiles.push_back(file);
    }
  }
  return partitionFiles;
}

void TestingPersistentShuffle::storePartitionBlock(int32_t partition) {
  auto& buffer = inProgressPartitions_[partition];
  auto file = getNextOutputFile(partition);
  file->append(
      std::string_view(buffer->as<char>(), inProgressSizes_[partition]));
  file->close();
  inProgressPartitions_[partition].reset();
  inProgressSizes_[partition] = 0;
}

void TestingPersistentShuffle::collect(
    int32_t partition,
    std::string_view data) {
  auto& buffer = inProgressPartitions_[partition];

  // Check if there is enough space in the buffer.
  if ((buffer != nullptr) &&
      (inProgressSizes_[partition] + data.size() + sizeof(size_t) >=
       buffer->capacity())) {
    storePartitionBlock(partition);
  }

  // Allocate buffer if needed.
  if (buffer == nullptr) {
    buffer = AlignedBuffer::allocate<char>(
        std::max(uint32_t(data.size()), maxBytesPerPartition_), pool_);
    inProgressSizes_[partition] = 0;
    inProgressPartitions_[partition] = buffer;
  }

  // Copy data.
  auto rawBuffer = buffer->asMutable<char>();
  auto offset = inProgressSizes_[partition];

  *(size_t*)(rawBuffer + offset) = data.size();

  offset += sizeof(size_t);
  ::memcpy(rawBuffer + offset, data.data(), data.size());

  inProgressSizes_[partition] += sizeof(size_t) + data.size();
}

void TestingPersistentShuffle::noMoreData(bool success) {
  // Delete all shuffle files on failure.
  if (!success) {
    cleanup();
  }
  for (auto i = 0; i < numPartitions_; ++i) {
    if (inProgressSizes_[i] > 0) {
      storePartitionBlock(i);
    }
  }
  // Write "ready to read" file.
  auto readyToRead = fileSystem_->openFileForWrite(
      fmt::format("{}/{}", rootPath_, kReadyForReadFilename));
  readyToRead->close();
}

bool TestingPersistentShuffle::hasNext(int32_t partition) const {
  while (!readyForRead()) {
    // This sleep is only for testing purposes.
    // For the test cases in which the shuffle reader tasks run before shuffle
    // writer tasks finish.
    sleep(1);
  }
  return readPartitionsFileIndex_[partition] <
      getReadPartitionFiles(partition).size();
}

BufferPtr TestingPersistentShuffle::next(int32_t partition, bool success) {
  // On failure, reset the index of the files to be read for that partition
  if (!success) {
    readPartitionsFileIndex_[partition] = 0;
  }
  if (readPartitionFiles_[partition].empty()) {
    readPartitionFiles_[partition] = getReadPartitionFiles(partition);
  }

  auto filename =
      readPartitionFiles_[partition][readPartitionsFileIndex_[partition]];
  auto file = fileSystem_->openFileForRead(filename);
  auto buffer = AlignedBuffer::allocate<char>(file->size(), pool_, 0);
  file->pread(0, file->size(), buffer->asMutable<void>());
  ++readPartitionsFileIndex_[partition];
  return buffer;
}

bool TestingPersistentShuffle::readyForRead() const {
  return fileSystem_->openFileForRead(
             fmt::format("{}/{}", rootPath_, kReadyForReadFilename)) != nullptr;
}

void TestingPersistentShuffle::cleanup() {
  auto files = fileSystem_->list(rootPath_);
  for (auto& file : files) {
    fileSystem_->remove(file);
  }
}
} // namespace facebook::presto::operators