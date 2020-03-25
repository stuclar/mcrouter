/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>
#include <utility>

#include <common/services/cpp/security/FizzAcceptor.h>
#include <wangle/acceptor/Acceptor.h>

namespace folly {
class EventBase;
} // namespace folly

namespace apache {
namespace thrift {
class ThriftServer;
} // namespace thrift
} // namespace apache

namespace facebook {
namespace memcache {

class ThriftAcceptorFactory final
    : public facebook::services::FizzAcceptorFactory {
  using ThriftAclCheckerFunc =
      std::function<bool(const folly::AsyncTransportWrapper*)>;

 public:
  explicit ThriftAcceptorFactory(
      apache::thrift::ThriftServer& server,
      ThriftAclCheckerFunc aclChecker,
      int trafficClass = 0)
      : facebook::services::FizzAcceptorFactory(&server),
        server_(server),
        aclChecker_(aclChecker),
        trafficClass_(trafficClass) {}
  ~ThriftAcceptorFactory() override = default;

  std::shared_ptr<wangle::Acceptor> newAcceptor(folly::EventBase* evb) override;

 private:
  apache::thrift::ThriftServer& server_;
  ThriftAclCheckerFunc aclChecker_;
  int trafficClass_;
};

} // namespace memcache
} // namespace facebook
