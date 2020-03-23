/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <signal.h>

#include <cstdio>

#include <folly/io/async/AsyncSignalHandler.h>
#include <folly/io/async/EventBase.h>

#include <thrift/lib/cpp2/server/ThriftServer.h>
#include <thrift/lib/cpp2/transport/rsocket/server/RSRoutingHandler.h>
#include "mcrouter/CarbonRouterClient.h"
#include "mcrouter/CarbonRouterInstance.h"
#include "mcrouter/McrouterLogFailure.h"
#include "mcrouter/OptionsUtil.h"
#include "mcrouter/Proxy.h"
#include "mcrouter/ProxyThread.h"
#include "mcrouter/ServerOnRequest.h"
#include "mcrouter/StandaloneConfig.h"
#include "mcrouter/ThriftAcceptor.h"
#include "mcrouter/config.h"
#include "mcrouter/lib/network/AsyncMcServer.h"
#include "mcrouter/lib/network/AsyncMcServerWorker.h"
#include "mcrouter/lib/network/Qos.h"
#include "mcrouter/standalone_options.h"

DECLARE_bool(dynamic_iothreadpoolexecutor);

namespace facebook {
namespace memcache {
namespace mcrouter {

namespace detail {

inline std::function<void(McServerSession&)> getAclChecker(
    const McrouterOptions& opts,
    const McrouterStandaloneOptions& standaloneOpts) {
  if (standaloneOpts.acl_checker_enable) {
    try {
      return getConnectionAclChecker(
          standaloneOpts.server_ssl_service_identity,
          standaloneOpts.acl_checker_enforce);
    } catch (const std::exception& ex) {
      MC_LOG_FAILURE(
          opts,
          failure::Category::kSystemError,
          "Error creating acl checker: {}",
          ex.what());
      LOG(WARNING) << "Disabling acl checker on all threads due to error.";
    }
  } else {
    LOG(WARNING) << "acl checker will not be enabled.";
  }
  return [](McServerSession&) {};
}

inline std::function<bool(const folly::AsyncTransportWrapper*)>
getThriftAclChecker(
    const McrouterOptions& opts,
    const McrouterStandaloneOptions& standaloneOpts) {
  if (standaloneOpts.acl_checker_enable) {
    try {
      return getThriftConnectionAclChecker(
          standaloneOpts.server_ssl_service_identity,
          standaloneOpts.acl_checker_enforce);
    } catch (const std::exception& ex) {
      MC_LOG_FAILURE(
          opts,
          failure::Category::kSystemError,
          "Error creating acl checker: {}",
          ex.what());
      LOG(WARNING) << "Disabling acl checker on all threads due to error.";
    }
  } else {
    LOG(WARNING) << "acl checker will not be enabled.";
  }
  return [](const folly::AsyncTransportWrapper*) { return true; };
}

template <class RouterInfo, template <class> class RequestHandler>
void serverInit(
    CarbonRouterInstance<RouterInfo>& router,
    size_t threadId,
    folly::EventBase& evb,
    AsyncMcServerWorker& worker,
    const McrouterStandaloneOptions& standaloneOpts,
    std::function<void(McServerSession&)>& aclChecker,
    CarbonRouterClient<RouterInfo>* routerClient) {
  using RequestHandlerType = RequestHandler<ServerOnRequest<RouterInfo>>;

  auto proxy = router.getProxy(threadId);
  // Manually override proxy assignment
  routerClient->setProxyIndex(threadId);

  worker.setOnRequest(RequestHandlerType(
      *routerClient,
      evb,
      standaloneOpts.retain_source_ip,
      standaloneOpts.enable_pass_through_mode,
      standaloneOpts.remote_thread));

  worker.setOnConnectionAccepted(
      [proxy, &aclChecker](McServerSession& session) mutable {
        proxy->stats().increment(num_client_connections_stat);
        try {
          aclChecker(session);
        } catch (const std::exception& ex) {
          MC_LOG_FAILURE(
              proxy->router().opts(),
              failure::Category::kSystemError,
              "Error running acl checker: {}",
              ex.what());
          LOG(WARNING) << "Disabling acl checker on this thread.";
          aclChecker = [](McServerSession&) {};
        }
      });
  worker.setOnConnectionCloseFinish(
      [proxy](McServerSession&, bool onAcceptedCalled) {
        if (onAcceptedCalled) {
          proxy->stats().decrement(num_client_connections_stat);
        }
      });

  // Setup compression on each worker.
  if (standaloneOpts.enable_server_compression) {
    auto codecManager = router.getCodecManager();
    if (codecManager) {
      worker.setCompressionCodecMap(codecManager->getCodecMap());
    } else {
      LOG(WARNING) << "Compression is enabled but couldn't find CodecManager. "
                   << "Compression will be disabled.";
    }
  }
}

template <class RouterInfo>
inline void startServerShutdown(
    std::shared_ptr<apache::thrift::ThriftServer> thriftServer,
    std::shared_ptr<AsyncMcServer> asyncMcServer,
    CarbonRouterInstance<RouterInfo>* router) {
  static std::atomic<bool> shutdownStarted{false};
  if (!shutdownStarted.exchange(true)) {
    LOG(INFO) << "Started server shutdown";
    if (asyncMcServer) {
      LOG(INFO) << "Started shutdown of AsyncMcServer";
      asyncMcServer->shutdown();
      asyncMcServer->join();
      asyncMcServer.reset();
      LOG(INFO) << "Completed shutdown of AsyncMcServer";
    }
    if (thriftServer) {
      LOG(INFO) << "Calling stop on ThriftServer";
      thriftServer->stop();
      LOG(INFO) << "Called stop on ThriftServer";
    }
    if (router) {
      LOG(INFO) << "Started shutdown of CarbonRouterInstance";
      router->shutdown();
      freeAllRouters();
      LOG(INFO) << "Completed shutdown of CarbonRouterInstance";
    }
  }
}

template <class RouterInfo, template <class> class RequestHandler>
void serverLoop(
    CarbonRouterInstance<RouterInfo>& router,
    size_t threadId,
    folly::EventBase& evb,
    AsyncMcServerWorker& worker,
    const McrouterStandaloneOptions& standaloneOpts,
    std::function<void(McServerSession&)>& aclChecker) {
  auto routerClient = standaloneOpts.remote_thread
      ? router.createClient(0 /* maximum_outstanding_requests */)
      : router.createSameThreadClient(0 /* maximum_outstanding_requests */);
  detail::serverInit<RouterInfo, RequestHandler>(
      router,
      threadId,
      evb,
      worker,
      standaloneOpts,
      aclChecker,
      routerClient.get());
  /* TODO(libevent): the only reason this is not simply evb.loop() is
     because we need to call asox stuff on every loop iteration.
     We can clean this up once we convert everything to EventBase */
  while (worker.isAlive() || worker.writesPending()) {
    evb.loopOnce();
  }
}

inline AsyncMcServer::Options createAsyncMcServerOptions(
    const McrouterOptions& mcrouterOpts,
    const McrouterStandaloneOptions& standaloneOpts,
    const std::vector<folly::EventBase*>* evb = nullptr) {
  AsyncMcServer::Options opts;

  if (standaloneOpts.listen_sock_fd >= 0) {
    opts.existingSocketFds = {standaloneOpts.listen_sock_fd};
  } else if (!standaloneOpts.unix_domain_sock.empty()) {
    opts.unixDomainSockPath = standaloneOpts.unix_domain_sock;
  } else {
    opts.listenAddresses = standaloneOpts.listen_addresses;
    opts.ports = standaloneOpts.ports;
    opts.sslPorts = standaloneOpts.ssl_ports;
    opts.tlsTicketKeySeedPath = standaloneOpts.tls_ticket_key_seed_path;
    opts.pemCertPath = standaloneOpts.server_pem_cert_path;
    opts.pemKeyPath = standaloneOpts.server_pem_key_path;
    opts.pemCaPath = standaloneOpts.server_pem_ca_path;
    opts.sslRequirePeerCerts = standaloneOpts.ssl_require_peer_certs;
    opts.tfoEnabledForSsl = mcrouterOpts.enable_ssl_tfo;
    opts.tfoQueueSize = standaloneOpts.tfo_queue_size;
    opts.worker.useKtls12 = standaloneOpts.ssl_use_ktls12;
  }

  opts.numThreads = mcrouterOpts.num_proxies;
  opts.numListeningSockets = standaloneOpts.num_listening_sockets;
  opts.worker.tcpZeroCopyThresholdBytes =
      standaloneOpts.tcp_zero_copy_threshold;

  size_t maxConns =
      opts.setMaxConnections(standaloneOpts.max_conns, opts.numThreads);
  if (maxConns > 0) {
    VLOG(1) << "The system will allow " << maxConns
            << " simultaneos connections before start closing connections"
            << " using an LRU algorithm";
  }

  if (standaloneOpts.enable_qos) {
    uint64_t qos = 0;
    if (getQoS(
            standaloneOpts.default_qos_class,
            standaloneOpts.default_qos_path,
            qos)) {
      opts.worker.trafficClass = qos;
    } else {
      VLOG(1) << "Incorrect qos class / qos path. Accepted connections will not"
              << "be marked.";
    }
  }

  opts.tcpListenBacklog = standaloneOpts.tcp_listen_backlog;
  opts.worker.defaultVersionHandler = false;
  opts.worker.maxInFlight = standaloneOpts.max_client_outstanding_reqs;
  opts.worker.sendTimeout =
      std::chrono::milliseconds{standaloneOpts.client_timeout_ms};
  if (!mcrouterOpts.debug_fifo_root.empty()) {
    opts.worker.debugFifoPath = getServerDebugFifoFullPath(mcrouterOpts);
  }

  if (standaloneOpts.server_load_interval_ms > 0) {
    opts.cpuControllerOpts.dataCollectionInterval =
        std::chrono::milliseconds(standaloneOpts.server_load_interval_ms);
  }

  /* Default to one read per event to help latency-sensitive workloads.
     We can make this an option if this needs to be adjusted. */
  opts.worker.maxReadsPerEvent = 1;

  if (evb) {
    opts.eventBases = (*evb);
  }
  return opts;
}

} // namespace detail

template <class RouterInfo>
class ShutdownSignalHandler : public folly::AsyncSignalHandler {
 public:
  explicit ShutdownSignalHandler(
      folly::EventBase* evb,
      std::shared_ptr<apache::thrift::ThriftServer> thriftServer,
      std::shared_ptr<AsyncMcServer> asyncMcServer,
      CarbonRouterInstance<RouterInfo>* router)
      : AsyncSignalHandler(evb),
        thriftServer_(thriftServer),
        asyncMcServer_(asyncMcServer),
        router_(router) {}

  void signalReceived(int) noexcept override {
    detail::startServerShutdown<RouterInfo>(
        thriftServer_, asyncMcServer_, router_);
  }

 private:
  std::shared_ptr<apache::thrift::ThriftServer> thriftServer_;
  std::shared_ptr<AsyncMcServer> asyncMcServer_;
  CarbonRouterInstance<RouterInfo>* router_;
};

class ExecutorObserver : public folly::ThreadPoolExecutor::Observer {
 public:
  void threadStarted(
      folly::ThreadPoolExecutor::ThreadHandle* threadHandle) override {
    CHECK(!initializationComplete_);
    evbs_.wlock()->push_back(
        folly::IOThreadPoolExecutor::getEventBase(threadHandle));
  }
  void threadPreviouslyStarted(
      folly::ThreadPoolExecutor::ThreadHandle* threadHandle) override {
    CHECK(!initializationComplete_);
    evbs_.wlock()->push_back(
        folly::IOThreadPoolExecutor::getEventBase(threadHandle));
  }

  void threadStopped(folly::ThreadPoolExecutor::ThreadHandle*) override {}

  std::vector<folly::EventBase*> extractEvbs() {
    CHECK(!std::exchange(initializationComplete_, true));
    return evbs_.exchange({});
  }

 private:
  bool initializationComplete_{false};
  folly::Synchronized<std::vector<folly::EventBase*>> evbs_;
};

template <class RouterInfo, template <class> class RequestHandler>
bool runServerDual(
    const McrouterOptions& mcrouterOpts,
    const McrouterStandaloneOptions& standaloneOpts,
    StandalonePreRunCb preRunCb) {
  using RequestHandlerType = RequestHandler<ServerOnRequest<RouterInfo>>;
  try {
    // Create thread pool for both AsyncMcServer and ThriftServer
    FLAGS_dynamic_iothreadpoolexecutor = false;
    std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool =
        std::make_shared<folly::IOThreadPoolExecutor>(mcrouterOpts.num_proxies);

    // Run observer and extract event bases
    auto executorObserver = std::make_shared<ExecutorObserver>();
    ioThreadPool->addObserver(executorObserver);
    std::vector<folly::EventBase*> evbs;
    for (auto& evb : executorObserver->extractEvbs()) {
      evbs.push_back(evb);
    }
    CHECK_EQ(evbs.size(), mcrouterOpts.num_proxies);
    ioThreadPool->removeObserver(executorObserver);

    // Create AsyncMcServer instance
    AsyncMcServer::Options opts =
        detail::createAsyncMcServerOptions(mcrouterOpts, standaloneOpts, &evbs);
    std::shared_ptr<AsyncMcServer> asyncMcServer =
        std::make_shared<AsyncMcServer>(opts);

    // Create CarbonRouterInstance
    CarbonRouterInstance<RouterInfo>* router;
    if (standaloneOpts.remote_thread) {
      router =
          CarbonRouterInstance<RouterInfo>::init("standalone", mcrouterOpts);
    } else {
      router = CarbonRouterInstance<RouterInfo>::init(
          "standalone", mcrouterOpts, evbs);
    }
    if (router == nullptr) {
      LOG(ERROR) << "CRITICAL: Failed to initialize mcrouter!";
      return false;
    }

    router->addStartupOpts(standaloneOpts.toDict());

    if (standaloneOpts.enable_server_compression &&
        !mcrouterOpts.enable_compression) {
      initCompression(*router);
    }

    if (preRunCb) {
      preRunCb(*router);
    }

    // Create CarbonRouterClients for each worker thread
    std::vector<typename CarbonRouterClient<RouterInfo>::Pointer>
        carbonRouterClients;
    std::unordered_map<
        folly::EventBase*,
        std::shared_ptr<ServerOnRequest<RouterInfo>>>
        serverOnRequestMap;
    int id = 0;
    for (auto evb : evbs) {
      // Create CarbonRouterClients
      auto routerClient = standaloneOpts.remote_thread
          ? router->createClient(0 /* maximum_outstanding_requests */)
          : router->createSameThreadClient(
                0 /* maximum_outstanding_requests */);
      routerClient->setProxyIndex(id++);

      serverOnRequestMap.emplace(
          evb,
          std::make_shared<ServerOnRequest<RouterInfo>>(
              *routerClient,
              *evb,
              standaloneOpts.retain_source_ip,
              standaloneOpts.enable_pass_through_mode,
              standaloneOpts.remote_thread));
      carbonRouterClients.push_back(std::move(routerClient));
    }
    CHECK_EQ(carbonRouterClients.size(), mcrouterOpts.num_proxies);
    CHECK_EQ(serverOnRequestMap.size(), mcrouterOpts.num_proxies);

    // Get local evb
    folly::EventBase* evb = ioThreadPool->getEventBaseManager()->getEventBase();

    // Thrift server setup
    apache::thrift::server::observerFactory_.reset();
    std::shared_ptr<apache::thrift::ThriftServer> thriftServer =
        std::make_shared<apache::thrift::ThriftServer>();
    thriftServer->setIOThreadPool(std::move(ioThreadPool));
    thriftServer->setNumCPUWorkerThreads(1);

    // Register signal handler which will handle ordered shutdown process of the
    // two servers
    ShutdownSignalHandler shutdownHandler(
        evb, thriftServer, asyncMcServer, router);
    shutdownHandler.registerSignalHandler(SIGTERM);
    shutdownHandler.registerSignalHandler(SIGINT);

    // Create thrift handler
    thriftServer->setInterface(
        std::make_shared<ServerOnRequestThrift<RouterInfo>>(
            serverOnRequestMap));

    // ACL Checker for ThriftServer
    auto aclCheckerThrift =
        detail::getThriftAclChecker(mcrouterOpts, standaloneOpts);

    uint64_t qos = 0;
    if (standaloneOpts.enable_qos) {
      if (!getQoS(
              standaloneOpts.default_qos_class,
              standaloneOpts.default_qos_path,
              qos)) {
        LOG(ERROR)
            << "Incorrect qos class / qos path. Accepted connections will not"
            << "be marked.";
      }
    }

    thriftServer->setAcceptorFactory(std::make_shared<ThriftAcceptorFactory>(
        *thriftServer, std::move(aclCheckerThrift), qos));

    // Set listening port for cleartext and SSL connections
    if (standaloneOpts.thrift_port > 0) {
      thriftServer->setPort(standaloneOpts.thrift_port);
    } else {
      LOG(ERROR) << "Must specify thrift port";
      router->shutdown();
      freeAllRouters();
      return false;
    }
    thriftServer->disableActiveRequestsTracking();
    thriftServer->setSocketMaxReadsPerEvent(1);
    // Set observer for connection stats
    // TODO: stuclar set observer
    //    thriftServer->setObserver(std::make_shared<MemcachedThriftObserver>());
    // Don't enforce default timeouts, unless Client forces them.
    thriftServer->setQueueTimeout(std::chrono::milliseconds(0));
    thriftServer->setTaskExpireTime(std::chrono::milliseconds(0));
    // Set idle and ssl handshake timeouts to 0 to be consistent with
    // AsyncMcServer
    thriftServer->setIdleServerTimeout(std::chrono::milliseconds(0));
    thriftServer->setSSLHandshakeTimeout(std::chrono::milliseconds(0));
    thriftServer->addRoutingHandler(
        std::make_unique<apache::thrift::RSRoutingHandler>());

    initStandaloneSSLDualServer(standaloneOpts, thriftServer);
    thriftServer->watchTicketPathForChanges(
        standaloneOpts.tls_ticket_key_seed_path, true);

    // Get acl checker for AsyncMcServer
    auto aclChecker = detail::getAclChecker(mcrouterOpts, standaloneOpts);
    // Start AsyncMcServer
    LOG(INFO) << "Starting AsyncMcServer in dual mode";
    asyncMcServer->startOnVirtualEB(
        [&carbonRouterClients,
         &router,
         &standaloneOpts,
         aclChecker = aclChecker](
            size_t threadId,
            folly::VirtualEventBase& vevb,
            AsyncMcServerWorker& worker) mutable {
          // Setup compression on each worker.
          if (standaloneOpts.enable_server_compression) {
            auto codecManager = router->getCodecManager();
            if (codecManager) {
              worker.setCompressionCodecMap(codecManager->getCodecMap());
            } else {
              LOG(WARNING)
                  << "Compression is enabled but couldn't find CodecManager. "
                  << "Compression will be disabled.";
            }
          }
          detail::serverInit<RouterInfo, RequestHandler>(
              *router,
              threadId,
              vevb.getEventBase(),
              worker,
              standaloneOpts,
              aclChecker,
              carbonRouterClients[threadId].get());
        },
        // Shutdown must be scheduled back to event base of main to ensure
        // that there we dont attempt to destruct a VirtualEventBase
        [evb = evb, &asyncMcServer, &thriftServer, &router] {
          evb->runInEventBaseThread([&]() {
            detail::startServerShutdown<RouterInfo>(
                thriftServer, asyncMcServer, router);
          });
        });

    LOG(INFO) << "Thrift Server and AsyncMcServer running.";
    // Run the ThriftServer; this blocks until the server is shut down.
    thriftServer->serve();
    thriftServer.reset();
    LOG(INFO) << "ThriftServer shutdown";
  } catch (const std::exception& e) {
    LOG(ERROR) << "Error creating dual mode AsyncMcServer: " << e.what();
    return false;
  }
  return true;
}

template <class RouterInfo, template <class> class RequestHandler>
bool runServer(
    const McrouterOptions& mcrouterOpts,
    const McrouterStandaloneOptions& standaloneOpts,
    StandalonePreRunCb preRunCb) {
  AsyncMcServer::Options opts =
      detail::createAsyncMcServerOptions(mcrouterOpts, standaloneOpts);

  try {
    LOG(INFO) << "Spawning AsyncMcServer";

    AsyncMcServer server(opts);
    server.installShutdownHandler({SIGINT, SIGTERM});

    CarbonRouterInstance<RouterInfo>* router = nullptr;

    SCOPE_EXIT {
      if (router) {
        router->shutdown();
      }
      server.join();

      LOG(INFO) << "Shutting down";

      freeAllRouters();

      if (!opts.unixDomainSockPath.empty()) {
        std::remove(opts.unixDomainSockPath.c_str());
      }
    };

    if (standaloneOpts.remote_thread) {
      router =
          CarbonRouterInstance<RouterInfo>::init("standalone", mcrouterOpts);
    } else {
      router = CarbonRouterInstance<RouterInfo>::init(
          "standalone", mcrouterOpts, server.eventBases());
    }
    if (router == nullptr) {
      LOG(ERROR) << "CRITICAL: Failed to initialize mcrouter!";
      return false;
    }

    router->addStartupOpts(standaloneOpts.toDict());

    if (standaloneOpts.enable_server_compression &&
        !mcrouterOpts.enable_compression) {
      initCompression(*router);
    }

    if (preRunCb) {
      preRunCb(*router);
    }

    auto aclChecker = detail::getAclChecker(mcrouterOpts, standaloneOpts);
    folly::Baton<> shutdownBaton;
    server.spawn(
        [router, &standaloneOpts, &aclChecker](
            size_t threadId,
            folly::EventBase& evb,
            AsyncMcServerWorker& worker) {
          detail::serverLoop<RouterInfo, RequestHandler>(
              *router, threadId, evb, worker, standaloneOpts, aclChecker);
        },
        [&shutdownBaton]() { shutdownBaton.post(); });

    shutdownBaton.wait();
  } catch (const std::exception& e) {
    LOG(ERROR) << e.what();
    return false;
  }
  return true;
}

} // namespace mcrouter
} // namespace memcache
} // namespace facebook
