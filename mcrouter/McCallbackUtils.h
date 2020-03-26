/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Optional.h>
#include <folly/io/async/EventBase.h>

#include <mcrouter/lib/network/gen/gen-cpp2/Memcache.h>

namespace facebook {
namespace memcache {

template <class Reply>
class McThriftCallback {
 public:
  McThriftCallback(std::unique_ptr<apache::thrift::HandlerCallback<Reply>> ctx)
      : underlying_(std::move(ctx)) {}

  static void reply(
      McThriftCallback<Reply>&& ctx,
      Reply&& reply,
      bool /* flush */ = false) {
    decltype(ctx.underlying_)::element_type::resultInThread(
        std::move(ctx.underlying_), std::move(reply));
  }

  folly::Optional<folly::StringPiece> getPeerSocketAddressStr() {
    folly::Optional<folly::StringPiece> peerAddressStr;
    auto connectionCxt = underlying_->getConnectionContext();
    if (connectionCxt) {
      peerAddressStr = connectionCxt->getPeerAddress()->getAddressStr();
    }
    return peerAddressStr;
  }

  folly::Optional<struct sockaddr_storage> getPeerSocketAddress() {
    folly::Optional<struct sockaddr_storage> peerAddress;
    auto connectionCxt = underlying_->getConnectionContext();
    if (connectionCxt && connectionCxt->getPeerAddress()) {
      peerAddress.emplace();
      connectionCxt->getPeerAddress()->getAddress(peerAddress.get_pointer());
    }
    return peerAddress;
  }

  folly::EventBase& getSessionEventBase() const noexcept {
    return *(underlying_->getEventBase());
  }

 private:
  std::unique_ptr<apache::thrift::HandlerCallback<Reply>> underlying_;
};

} // namespace memcache
} // namespace facebook
