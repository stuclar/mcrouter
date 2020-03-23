/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/io/async/EventBase.h>
#include <cassert>

#include <mcrouter/lib/network/gen/Memcache.h>
#include <mcrouter/lib/network/gen/gen-cpp2/Memcache.h>
#include "mcrouter/CarbonRouterClient.h"
#include "mcrouter/McCallbackUtils.h"
#include "mcrouter/config.h"
#include "mcrouter/lib/network/AsyncMcServer.h"
#include "mcrouter/lib/network/AsyncMcServerWorker.h"
#include "mcrouter/lib/network/CaretHeader.h"
#include "mcrouter/lib/network/McServerRequestContext.h"
#include "mcrouter/lib/network/gen/MemcacheMessages.h"

namespace facebook {
namespace memcache {
namespace mcrouter {

template <class Callback, class Request>
struct ServerRequestContext {
  Callback ctx;
  Request req;
  folly::IOBuf reqBuffer;

  ServerRequestContext(
      Callback&& ctx_,
      Request&& req_,
      const folly::IOBuf* reqBuffer_)
      : ctx(std::move(ctx_)),
        req(std::move(req_)),
        reqBuffer(reqBuffer_ ? reqBuffer_->cloneAsValue() : folly::IOBuf()) {}
};

template <class RouterInfo>
class ServerOnRequest {
 public:
  template <class Callback, class Request>
  using ReplyFunction =
      void (*)(Callback&& ctx, ReplyT<Request>&& reply, bool flush);

  ServerOnRequest(
      CarbonRouterClient<RouterInfo>& client,
      folly::EventBase& eventBase,
      bool retainSourceIp,
      bool enablePassThroughMode,
      bool remoteThread)
      : client_(client),
        eventBase_(eventBase),
        retainSourceIp_(retainSourceIp),
        enablePassThroughMode_(enablePassThroughMode),
        remoteThread_(remoteThread) {}

  template <class Reply, class Callback>
  void sendReply(Callback&& ctx, Reply&& reply) {
    if (remoteThread_) {
      return eventBase_.runInEventBaseThread(
          [ctx = std::move(ctx), reply = std::move(reply)]() mutable {
            Callback::reply(std::move(ctx), std::move(reply));
          });
    } else {
      return Callback::reply(std::move(ctx), std::move(reply));
    }
  }

  template <class Request, class Callback>
  void onRequest(
      Callback&& ctx,
      Request&& req,
      const CaretMessageInfo* headerInfo,
      const folly::IOBuf* reqBuffer) {
    using Reply = ReplyT<Request>;
    send(
        std::move(ctx),
        std::move(req),
        &Callback::template reply<Reply>,
        headerInfo,
        reqBuffer);
  }

  template <class Request>
  void onRequestThrift(McThriftCallback<ReplyT<Request>>&& ctx, Request&& req) {
    send(
        std::move(ctx),
        std::move(req),
        &McThriftCallback<ReplyT<Request>>::reply);
  }

  template <class Request, class Callback>
  void onRequest(Callback&& ctx, Request&& req) {
    using Reply = ReplyT<Request>;
    send(std::move(ctx), std::move(req), &Callback::template reply<Reply>);
  }

  template <class Callback>
  void onRequest(Callback&& ctx, McVersionRequest&&) {
    McVersionReply reply(carbon::Result::OK);
    reply.value() =
        folly::IOBuf(folly::IOBuf::COPY_BUFFER, MCROUTER_PACKAGE_STRING);

    sendReply(std::move(ctx), std::move(reply));
  }

  template <class Callback>
  void onRequest(Callback&& ctx, McQuitRequest&&) {
    sendReply(std::move(ctx), McQuitReply(carbon::Result::OK));
  }

  template <class Callback>
  void onRequest(Callback&& ctx, McShutdownRequest&&) {
    sendReply(std::move(ctx), McShutdownReply(carbon::Result::OK));
  }

  template <class Callback, class Request>
  void send(
      Callback&& ctx,
      Request&& req,
      ReplyFunction<Callback, Request> replyFn,
      const CaretMessageInfo* headerInfo = nullptr,
      const folly::IOBuf* reqBuffer = nullptr) {
    // We just reuse buffers iff:
    //  1) enablePassThroughMode_ is true.
    //  2) headerInfo is not NULL.
    //  3) reqBuffer is not NULL.
    const folly::IOBuf* reusableRequestBuffer =
        (enablePassThroughMode_ && headerInfo) ? reqBuffer : nullptr;

    auto rctx = std::make_unique<ServerRequestContext<Callback, Request>>(
        std::move(ctx), std::move(req), reusableRequestBuffer);
    auto& reqRef = rctx->req;
    // TODO STUCLAR PUT BACK
    // auto& sessionRef = rctx->ctx.session();

    // if we are reusing the request buffer, adjust the start offset and set
    // it to the request.
    if (reusableRequestBuffer) {
      auto& reqBufferRef = rctx->reqBuffer;
      reqBufferRef.trimStart(headerInfo->headerSize);
      reqRef.setSerializedBuffer(reqBufferRef);
    }

    auto cb = [this, sctx = std::move(rctx), replyFn = std::move(replyFn)](
                  const Request&, ReplyT<Request>&& reply) mutable {
      if (remoteThread_) {
        eventBase_.runInEventBaseThread([sctx = std::move(sctx),
                                         replyFn = std::move(replyFn),
                                         reply = std::move(reply)]() mutable {
          replyFn(std::move(sctx->ctx), std::move(reply), false /* flush */);
        });
      } else {
        replyFn(std::move(sctx->ctx), std::move(reply), false /* flush */);
      }
    };

    if (retainSourceIp_) {
      // TODO STUCLAR PUT BACK
      // auto peerIp = sessionRef.getSocketAddress().getAddressStr();
      // client_.send(reqRef, std::move(cb), peerIp);
      client_.send(reqRef, std::move(cb));
    } else {
      client_.send(reqRef, std::move(cb));
    }
  }

 private:
  CarbonRouterClient<RouterInfo>& client_;
  folly::EventBase& eventBase_;
  const bool retainSourceIp_{false};
  const bool enablePassThroughMode_{false};
  const bool remoteThread_{false};
};

template <class RouterInfo>
class ServerOnRequestThrift : public thrift::MemcacheSvIf {
 public:
  ServerOnRequestThrift(
      std::unordered_map<
          folly::EventBase*,
          std::shared_ptr<ServerOnRequest<RouterInfo>>> serverOnRequestMap)
      : serverOnRequestMap_(serverOnRequestMap) {}

  // thrift
  void async_eb_mcVersion(
      std::unique_ptr<apache::thrift::HandlerCallback<McVersionReply>> callback,
      const McVersionRequest& request) override {
    getServerOnRequest(callback->getEventBase())
        ->onRequestThrift(
            McThriftCallback<McVersionReply>(std::move(callback)),
            std::move(const_cast<McVersionRequest&>(request)));
  }
  void async_eb_mcGet(
      std::unique_ptr<apache::thrift::HandlerCallback<McGetReply>> callback,
      const McGetRequest& request) override {
    getServerOnRequest(callback->getEventBase())
        ->onRequestThrift(
            McThriftCallback<McGetReply>(std::move(callback)),
            std::move(const_cast<McGetRequest&>(request)));
  }

  void async_eb_mcSet(
      std::unique_ptr<apache::thrift::HandlerCallback<McSetReply>> callback,
      const McSetRequest& request) override {
    getServerOnRequest(callback->getEventBase())
        ->onRequestThrift(
            McThriftCallback<McSetReply>(std::move(callback)),
            std::move(const_cast<McSetRequest&>(request)));
  }

  void async_eb_mcDelete(
      std::unique_ptr<apache::thrift::HandlerCallback<McDeleteReply>> callback,
      const McDeleteRequest& request) override {
    getServerOnRequest(callback->getEventBase())
        ->onRequestThrift(
            McThriftCallback<McDeleteReply>(std::move(callback)),
            std::move(const_cast<McDeleteRequest&>(request)));
  }

  void async_eb_mcLeaseGet(
      std::unique_ptr<apache::thrift::HandlerCallback<McLeaseGetReply>>
          callback,
      const McLeaseGetRequest& request) override {
    getServerOnRequest(callback->getEventBase())
        ->onRequestThrift(
            McThriftCallback<McLeaseGetReply>(std::move(callback)),
            std::move(const_cast<McLeaseGetRequest&>(request)));
  }

  void async_eb_mcLeaseSet(
      std::unique_ptr<apache::thrift::HandlerCallback<McLeaseSetReply>>
          callback,
      const McLeaseSetRequest& request) override {
    getServerOnRequest(callback->getEventBase())
        ->onRequestThrift(
            McThriftCallback<McLeaseSetReply>(std::move(callback)),
            std::move(const_cast<McLeaseSetRequest&>(request)));
  }

  void async_eb_mcAdd(
      std::unique_ptr<apache::thrift::HandlerCallback<McAddReply>> callback,
      const McAddRequest& request) override {
    getServerOnRequest(callback->getEventBase())
        ->onRequestThrift(
            McThriftCallback<McAddReply>(std::move(callback)),
            std::move(const_cast<McAddRequest&>(request)));
  }

  void async_eb_mcReplace(
      std::unique_ptr<apache::thrift::HandlerCallback<McReplaceReply>> callback,
      const McReplaceRequest& request) override {
    getServerOnRequest(callback->getEventBase())
        ->onRequestThrift(
            McThriftCallback<McReplaceReply>(std::move(callback)),
            std::move(const_cast<McReplaceRequest&>(request)));
  }

  void async_eb_mcGets(
      std::unique_ptr<apache::thrift::HandlerCallback<McGetsReply>> callback,
      const McGetsRequest& request) override {
    getServerOnRequest(callback->getEventBase())
        ->onRequestThrift(
            McThriftCallback<McGetsReply>(std::move(callback)),
            std::move(const_cast<McGetsRequest&>(request)));
  }

  void async_eb_mcCas(
      std::unique_ptr<apache::thrift::HandlerCallback<McCasReply>> callback,
      const McCasRequest& request) override {
    getServerOnRequest(callback->getEventBase())
        ->onRequestThrift(
            McThriftCallback<McCasReply>(std::move(callback)),
            std::move(const_cast<McCasRequest&>(request)));
  }

  void async_eb_mcIncr(
      std::unique_ptr<apache::thrift::HandlerCallback<McIncrReply>> callback,
      const McIncrRequest& request) override {
    getServerOnRequest(callback->getEventBase())
        ->onRequestThrift(
            McThriftCallback<McIncrReply>(std::move(callback)),
            std::move(const_cast<McIncrRequest&>(request)));
  }

  void async_eb_mcDecr(
      std::unique_ptr<apache::thrift::HandlerCallback<McDecrReply>> callback,
      const McDecrRequest& request) override {
    getServerOnRequest(callback->getEventBase())
        ->onRequestThrift(
            McThriftCallback<McDecrReply>(std::move(callback)),
            std::move(const_cast<McDecrRequest&>(request)));
  }

  void async_eb_mcMetaget(
      std::unique_ptr<apache::thrift::HandlerCallback<McMetagetReply>> callback,
      const McMetagetRequest& request) override {
    getServerOnRequest(callback->getEventBase())
        ->onRequestThrift(
            McThriftCallback<McMetagetReply>(std::move(callback)),
            std::move(const_cast<McMetagetRequest&>(request)));
  }

  void async_eb_mcAppend(
      std::unique_ptr<apache::thrift::HandlerCallback<McAppendReply>> callback,
      const McAppendRequest& request) override {
    getServerOnRequest(callback->getEventBase())
        ->onRequestThrift(
            McThriftCallback<McAppendReply>(std::move(callback)),
            std::move(const_cast<McAppendRequest&>(request)));
  }

  void async_eb_mcPrepend(
      std::unique_ptr<apache::thrift::HandlerCallback<McPrependReply>> callback,
      const McPrependRequest& request) override {
    getServerOnRequest(callback->getEventBase())
        ->onRequestThrift(
            McThriftCallback<McPrependReply>(std::move(callback)),
            std::move(const_cast<McPrependRequest&>(request)));
  }

  void async_eb_mcTouch(
      std::unique_ptr<apache::thrift::HandlerCallback<McTouchReply>> callback,
      const McTouchRequest& request) override {
    getServerOnRequest(callback->getEventBase())
        ->onRequestThrift(
            McThriftCallback<McTouchReply>(std::move(callback)),
            std::move(const_cast<McTouchRequest&>(request)));
  }

  void async_eb_mcFlushRe(
      std::unique_ptr<apache::thrift::HandlerCallback<McFlushReReply>> callback,
      const McFlushReRequest& request) override {
    getServerOnRequest(callback->getEventBase())
        ->onRequestThrift(
            McThriftCallback<McFlushReReply>(std::move(callback)),
            std::move(const_cast<McFlushReRequest&>(request)));
  }

  void async_eb_mcFlushAll(
      std::unique_ptr<apache::thrift::HandlerCallback<McFlushAllReply>>
          callback,
      const McFlushAllRequest& request) override {
    getServerOnRequest(callback->getEventBase())
        ->onRequestThrift(
            McThriftCallback<McFlushAllReply>(std::move(callback)),
            std::move(const_cast<McFlushAllRequest&>(request)));
  }

  void async_eb_mcGat(
      std::unique_ptr<apache::thrift::HandlerCallback<McGatReply>> callback,
      const McGatRequest& request) override {
    getServerOnRequest(callback->getEventBase())
        ->onRequestThrift(
            McThriftCallback<McGatReply>(std::move(callback)),
            std::move(const_cast<McGatRequest&>(request)));
  }

  void async_eb_mcGats(
      std::unique_ptr<apache::thrift::HandlerCallback<McGatsReply>> callback,
      const McGatsRequest& request) override {
    getServerOnRequest(callback->getEventBase())
        ->onRequestThrift(
            McThriftCallback<McGatsReply>(std::move(callback)),
            std::move(const_cast<McGatsRequest&>(request)));
  }

  void async_eb_mcExec(
      std::unique_ptr<apache::thrift::HandlerCallback<McExecReply>> callback,
      const McExecRequest& request) {
    getServerOnRequest(callback->getEventBase())
        ->onRequestThrift(
            McThriftCallback<McExecReply>(std::move(callback)),
            std::move(const_cast<McExecRequest&>(request)));
  }

  // Return this factory instead of MemcacheAsyncProcessor from getProcessor(),
  // so that we don't use the default statically registered handlers
  class MemcacheAsyncProcessorCustomHandlers
      : public thrift::MemcacheAsyncProcessor {
   public:
    explicit MemcacheAsyncProcessorCustomHandlers(thrift::MemcacheSvIf* svif)
        : MemcacheAsyncProcessor(svif) {
      clearEventHandlers();
    }
  };
  std::unique_ptr<apache::thrift::AsyncProcessor> getProcessor() override {
    return std::make_unique<MemcacheAsyncProcessorCustomHandlers>(this);
  }

 private:
  std::unordered_map<
      folly::EventBase*,
      std::shared_ptr<ServerOnRequest<RouterInfo>>>
      serverOnRequestMap_;
  static thread_local ServerOnRequest<RouterInfo>* serverOnRequest_;

  // Returns the ServerOnRequest* associated with this evb or throws
  // std::logic_error if not found.
  ServerOnRequest<RouterInfo>* getServerOnRequest(folly::EventBase* evb) {
    if (serverOnRequest_ == nullptr) {
      auto it = serverOnRequestMap_.find(evb);
      assert(it != serverOnRequestMap_.end());
      serverOnRequest_ = it->second.get();
    }
    return serverOnRequest_;
  }
};

template <class RouterInfo>
thread_local ServerOnRequest<RouterInfo>*
    ServerOnRequestThrift<RouterInfo>::serverOnRequest_{nullptr};

} // namespace mcrouter
} // namespace memcache
} // namespace facebook
