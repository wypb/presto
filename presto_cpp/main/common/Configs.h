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
#pragma once

#include <chrono>
#include <memory>
#include <string>
#include <unordered_map>
#include "velox/core/Context.h"

namespace facebook::presto {

class ConfigBase {
 public:
  /// Reads configuration properties from the specified file. Must be called
  /// before calling any of the getters below.
  /// @param filePath Path to configuration file.
  void initialize(const std::string& filePath);

  template <typename T>
  T requiredProperty(const std::string& propertyName) const {
    auto propertyValue = config_->get<T>(propertyName);
    if (propertyValue.has_value()) {
      return propertyValue.value();
    } else {
      VELOX_FAIL("{} is required in the {} file.", propertyName, filePath_);
    }
  }

  std::string requiredProperty(const std::string& propertyName) const {
    auto propertyValue = config_->get(propertyName);
    if (propertyValue.has_value()) {
      return propertyValue.value();
    } else {
      VELOX_FAIL("{} is required in the {} file.", propertyName, filePath_);
    }
  }

  template <typename T>
  folly::Optional<T> optionalProperty(const std::string& propertyName) const {
    return config_->get<T>(propertyName);
  }

  folly::Optional<std::string> optionalProperty(
      const std::string& propertyName) const {
    return config_->get(propertyName);
  }

  const std::unordered_map<std::string, std::string>& values() const {
    return config_->values();
  }

 protected:
  ConfigBase();

  std::unique_ptr<velox::Config> config_;
  std::string filePath_;
};

/// Provides access to system properties defined in config.properties file.
class SystemConfig : public ConfigBase {
 public:
  static constexpr std::string_view kPrestoVersion{"presto.version"};
  static constexpr std::string_view kHttpServerHttpPort{
      "http-server.http.port"};
  static constexpr std::string_view kDiscoveryUri{"discovery.uri"};
  static constexpr std::string_view kMaxDriversPerTask{
      "task.max-drivers-per-task"};
  static constexpr std::string_view kConcurrentLifespansPerTask{
      "task.concurrent-lifespans-per-task"};
  static constexpr int32_t kMaxDriversPerTaskDefault = 5;
  static constexpr int32_t kConcurrentLifespansPerTaskDefault = 1;

  static SystemConfig* instance();

  int httpServerHttpPort() const;

  std::string prestoVersion() const;

  std::string discoveryUri() const;

  int32_t maxDriversPerTask() const;

  int32_t concurrentLifespansPerTask() const;
};

/// Provides access to node properties defined in node.properties file.
class NodeConfig : public ConfigBase {
 public:
  static constexpr std::string_view kNodeEnvironment{"node.environment"};
  static constexpr std::string_view kNodeId{"node.id"};
  static constexpr std::string_view kNodeIp{"node.ip"};
  static constexpr std::string_view kNodeLocation{"node.location"};
  static constexpr std::string_view kNodeMemoryGb{"node.memory_gb"};

  static NodeConfig* instance();

  std::string nodeEnvironment() const;

  std::string nodeId() const;

  std::string nodeIp(
      const std::function<std::string()>& defaultIp = nullptr) const;

  std::string nodeLocation() const;

  uint64_t nodeMemoryGb(
      const std::function<uint64_t()>& defaultNodeMemoryGb = nullptr) const;
};

} // namespace facebook::presto
