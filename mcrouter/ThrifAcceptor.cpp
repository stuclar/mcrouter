/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <mcrouter/ThriftAcceptor.h>

#include <memory>
#include <string>
#include <utility>

#include <fmt/core.h>

#include <folly/GLog.h>
#include <folly/io/async/AsyncSSLSocket.h>

#include <mcrouter/lib/network/McSSLUtil.h>
#include <mcrouter/lib/network/SecurityOptions.h>
#include <thrift/lib/cpp2/server/Cpp2Worker.h>
#include <wangle/acceptor/SecureTransportType.h>

namespace folly {
class SocketAddress;
} // namespace folly

namespace facebook {
namespace memcache {

std::shared_ptr<wangle::Acceptor> ThriftAcceptorFactory::newAcceptor(
    folly::EventBase* evb) {
  class ThriftAcceptor : public apache::thrift::Cpp2Worker {
    using ThriftAclCheckerFunc = ThriftAcceptorFactory::ThriftAclCheckerFunc;

   public:
    ThriftAcceptor(
        apache::thrift::ThriftServer& server,
        folly::EventBase* evb,
        ThriftAclCheckerFunc aclChecker)
        : apache::thrift::Cpp2Worker(&server, {}) {
      construct(&server, nullptr, evb);
    }

    static std::shared_ptr<ThriftAcceptor> create(
        apache::thrift::ThriftServer& server,
        folly::EventBase* evb,
        ThriftAclCheckerFunc aclChecker,
        int trafficClass = 0) {
      auto self = std::make_shared<ThriftAcceptor>(
          server, evb, aclChecker, trafficClass);
      self->construct(&server, nullptr, evb);
      return self;
    }

    void onNewConnection(
        folly::AsyncTransportWrapper::UniquePtr socket,
        const folly::SocketAddress* address,
        const std::string& nextProtocolName,
        wangle::SecureTransportType secureTransportType,
        const wangle::TransportInfo& transportInfo) final {
      if (!nextProtocolName.empty() ||
          secureTransportType != wangle::SecureTransportType::NONE) {
        // We can only handle plaintext connections.
        FB_LOG_EVERY_MS(ERROR, 5000) << fmt::format(
            "Dropping new connection with SecureTransportType '{}' and"
            " nextProtocolName '{}'",
            wangle::getSecureTransportName(secureTransportType),
            nextProtocolName);
        return;
      }

      // Ensure socket has same options that would be applied in AsyncMcServer
      auto* asyncSocket = socket->getUnderlyingTransport<folly::AsyncSocket>();
      asyncSocket->setNoDelay(true);
      asyncSocket->setSendTimeout(0);
      if (trafficClass_ &&
          asyncSocket->setSockOpt(IPPROTO_IPV6, IPV6_TCLASS, &trafficClass_) !=
              0) {
        LOG_EVERY_N(ERROR, 5000)
            << "Failed to set TCLASS = " << trafficClass_ << " on socket";
      }
      apache::thrift::Cpp2Worker::onNewConnection(
          std::move(socket), address, "", secureTransportType, transportInfo);
    }

   private:
    ThriftAclCheckerFunc aclChecker_;
  };
  return ThriftAcceptor::create(server_, evb, aclChecker_, trafficClass_);
}

} // namespace memcache
} // namespace facebook
