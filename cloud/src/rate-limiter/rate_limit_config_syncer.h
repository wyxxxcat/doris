// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <gen_cpp/cloud.pb.h>

#include <atomic>
#include <memory>
#include <thread>

namespace doris::cloud {

class TxnKv;
class RateLimiter;

// FDB key for rate limit config
constexpr std::string_view RATE_LIMIT_CONFIG_KEY = "rate_limit_config";

// Syncs rate limit configuration from FDB to local RateLimiter
class RateLimitConfigSyncer {
public:
    RateLimitConfigSyncer(std::shared_ptr<TxnKv> txn_kv,
                          std::shared_ptr<RateLimiter> rate_limiter);
    ~RateLimitConfigSyncer();

    // Start background sync thread
    int start();

    // Stop background sync thread
    void stop();

    // Manually trigger sync
    void sync_config();

    // Save config to FDB
    bool save_config(const RateLimitConfigPB& config);

    // Load config from FDB
    bool load_config(RateLimitConfigPB& config);

    // Get current config version
    int64_t current_version() const { return current_version_.load(); }

private:
    void sync_loop();
    void apply_config(const RateLimitConfigPB& config);

    std::shared_ptr<TxnKv> txn_kv_;
    std::shared_ptr<RateLimiter> rate_limiter_;
    std::atomic<int64_t> current_version_{0};
    std::atomic<bool> running_{false};
    std::unique_ptr<std::thread> sync_thread_;
};

} // namespace doris::cloud
