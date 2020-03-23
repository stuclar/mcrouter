/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/io/async/AsyncTransport.h>
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include <functional>
#include <unordered_map>

namespace facebook {
namespace memcache {

// forward declarations
class McrouterOptions;
class McServerSession;

namespace mcrouter {
// forward declarations
class McrouterStandaloneOptions;

void standalonePreInitFromCommandLineOpts(
    const std::unordered_map<std::string, std::string>& standaloneOptionsDict);

void standaloneInit(
    const McrouterOptions& opts,
    const McrouterStandaloneOptions& standaloneOpts);

void initStandaloneSSL();

void initStandaloneSSLDualServer(
    const McrouterStandaloneOptions& standaloneOpts,
    std::shared_ptr<apache::thrift::ThriftServer> thriftServer);

void finalizeStandaloneOptions(McrouterStandaloneOptions& opts);

std::function<void(McServerSession&)> getConnectionAclChecker(
    const std::string& serviceIdentity,
    bool enforce);

std::function<bool(const folly::AsyncTransportWrapper*)>
getThriftConnectionAclChecker(const std::string& serviceIdentity, bool enforce);

} // namespace mcrouter
} // namespace memcache
} // namespace facebook
