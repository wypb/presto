// DO NOT EDIT : This file is generated by presto_protocol-to-thrift-json.py
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

// This file is generated DO NOT EDIT @generated
// This file is generated DO NOT EDIT @generated

#include "presto_cpp/main/thrift/ProtocolToThrift.h"

namespace facebook::presto {

// These could be covered by a more general template but this way only
// conversions to supported Thrift data types can be generated.
void toThrift(const std::string& proto, std::string& thrift) {
  thrift = proto;
}
void toThrift(const bool& proto, bool& thrift) {
  thrift = proto;
}
void toThrift(const int32_t& proto, int32_t& thrift) {
  thrift = proto;
}
void toThrift(const int64_t& proto, int64_t& thrift) {
  thrift = proto;
}
void toThrift(const double& proto, double& thrift) {
  thrift = proto;
}

template <typename P, typename T>
void toThrift(const std::shared_ptr<P>& proto, std::shared_ptr<T>& thrift) {
  if (proto) {
    thrift = std::make_shared<T>();
    toThrift(*proto, *thrift);
  }
}

template <typename V, typename S>
void toThrift(const std::vector<V>& v, std::set<S>& s) {
  S toItem;
  for (const auto& fromItem : v) {
    toThrift(fromItem, toItem);
    s.insert(toItem);
  }
}

template <typename P, typename T>
void toThrift(const std::vector<P>& p, std::vector<T>& t) {
  T toItem;
  for (const auto& fromItem : p) {
    toThrift(fromItem, toItem);
    t.push_back(toItem);
  }
}

void toThrift(const protocol::TaskState& proto, thrift::TaskState& thrift) {
  thrift = (thrift::TaskState)((int)proto);
}
void toThrift(const protocol::ErrorType& proto, thrift::ErrorType& thrift) {
  thrift = (thrift::ErrorType)((int)proto);
}
void toThrift(const protocol::Lifespan& proto, thrift::Lifespan& thrift) {
  toThrift(proto.isgroup, *thrift.grouped());
  toThrift(proto.groupid, *thrift.groupId());
}
void toThrift(
    const protocol::ErrorLocation& proto,
    thrift::ErrorLocation& thrift) {
  toThrift(proto.lineNumber, *thrift.lineNumber());
  toThrift(proto.columnNumber, *thrift.columnNumber());
}
void toThrift(const protocol::HostAddress& proto, thrift::HostAddress& thrift) {
  std::vector<std::string> parts;
  folly::split(":", proto, parts);
  if (parts.size() == 2) {
    thrift.host() = parts[0];
    thrift.port() = std::stoi(parts[1]);
  }
}
void toThrift(const protocol::TaskStatus& proto, thrift::TaskStatus& thrift) {
  toThrift(
      proto.taskInstanceIdLeastSignificantBits,
      *thrift.taskInstanceIdLeastSignificantBits());
  toThrift(
      proto.taskInstanceIdMostSignificantBits,
      *thrift.taskInstanceIdMostSignificantBits());
  toThrift(proto.version, *thrift.version());
  toThrift(proto.state, *thrift.state());
  toThrift(proto.self, *thrift.taskName());
  toThrift(proto.completedDriverGroups, *thrift.completedDriverGroups());
  toThrift(proto.failures, *thrift.failures());
  toThrift(
      proto.queuedPartitionedDrivers, *thrift.queuedPartitionedDrivers());
  toThrift(
      proto.runningPartitionedDrivers, *thrift.runningPartitionedDrivers());
  toThrift(
      proto.outputBufferUtilization, *thrift.outputBufferUtilization());
  toThrift(
      proto.outputBufferOverutilized, *thrift.outputBufferOverutilized());
  toThrift(
      proto.physicalWrittenDataSizeInBytes,
      *thrift.physicalWrittenDataSizeInBytes());
  toThrift(
      proto.memoryReservationInBytes, *thrift.memoryReservationInBytes());
  toThrift(
      proto.systemMemoryReservationInBytes,
      *thrift.systemMemoryReservationInBytes());
  toThrift(proto.fullGcCount, *thrift.fullGcCount());
  toThrift(proto.fullGcTimeInMillis, *thrift.fullGcTimeInMillis());
  toThrift(
      proto.peakNodeTotalMemoryReservationInBytes,
      *thrift.peakNodeTotalMemoryReservationInBytes());
}
void toThrift(const protocol::ErrorCode& proto, thrift::ErrorCode& thrift) {
  toThrift(proto.code, *thrift.code());
  toThrift(proto.name, *thrift.name());
  toThrift(proto.type, *thrift.type());
}
void toThrift(
    const protocol::ExecutionFailureInfo& proto,
    thrift::ExecutionFailureInfo& thrift) {
  toThrift(proto.type, *thrift.type());
  toThrift(proto.message, *thrift.message());
  toThrift(proto.cause, thrift.cause_ref());
  toThrift(proto.suppressed, *thrift.suppressed());
  toThrift(proto.stack, *thrift.stack());
  toThrift(proto.errorLocation, *thrift.errorLocation());
  toThrift(proto.errorCode, *thrift.errorCode());
  toThrift(proto.remoteHost, *thrift.remoteHost());
}

} // namespace facebook::presto
