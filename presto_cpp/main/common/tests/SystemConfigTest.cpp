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
#include <gtest/gtest.h>
#include "presto_cpp/main/common/Configs.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"

namespace facebook::presto::test {

using namespace velox;

class SystemConfigTest : public testing::Test {
 protected:
  void setUpConfigFile(bool isMutable) {
    velox::filesystems::registerLocalFileSystem();

    char path[] = "/tmp/velox_system_config_test_XXXXXX";
    const char* tempDirectoryPath = mkdtemp(path);
    if (tempDirectoryPath == nullptr) {
      throw std::logic_error("Cannot open temp directory");
    }
    configFilePath = tempDirectoryPath;
    configFilePath += "/config.properties";

    auto fileSystem = filesystems::getFileSystem(configFilePath, nullptr);
    auto sysConfigFile = fileSystem->openFileForWrite(configFilePath);
    sysConfigFile->append(
        fmt::format("{}={}\n", SystemConfig::kPrestoVersion, prestoVersion));
    if (isMutable) {
      sysConfigFile->append(
          fmt::format("{}={}\n", SystemConfig::kMutableConfig, "true"));
    }
    sysConfigFile->close();
  }

  std::string configFilePath;
  const std::string prestoVersion{"SystemConfigTest1"};
  const std::string prestoVersion2{"SystemConfigTest2"};
};

TEST_F(SystemConfigTest, defaultConfig) {
  setUpConfigFile(false);
  auto systemConfig = SystemConfig::instance();
  systemConfig->initialize(configFilePath);

  ASSERT_FALSE(
      systemConfig
          ->optionalProperty<bool>(std::string{SystemConfig::kMutableConfig})
          .has_value());
  ASSERT_EQ(prestoVersion, systemConfig->prestoVersion());
  ASSERT_THROW(
      systemConfig->setValue(
          std::string(SystemConfig::kPrestoVersion), prestoVersion2),
      VeloxException);
}

TEST_F(SystemConfigTest, mutableConfig) {
  setUpConfigFile(true);
  auto systemConfig = SystemConfig::instance();
  systemConfig->initialize(configFilePath);

  ASSERT_TRUE(
      systemConfig
          ->optionalProperty<bool>(std::string{SystemConfig::kMutableConfig})
          .value());
  ASSERT_EQ(prestoVersion, systemConfig->prestoVersion());
  ASSERT_EQ(
      prestoVersion,
      systemConfig
          ->setValue(std::string(SystemConfig::kPrestoVersion), prestoVersion2)
          .value());
  ASSERT_EQ(prestoVersion2, systemConfig->prestoVersion());
}

} // namespace facebook::presto::test