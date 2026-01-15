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

#include "rate_limit_config_syncer.h"

#include <chrono>

#include "common/config.h"
#include "common/logging.h"
#include "meta-store/txn_kv.h"
#include "rate_limiter.h"

namespace doris::cloud {

RateLimitConfigSyncer::RateLimitConfigSyncer(std::shared_ptr<TxnKv> txn_kv,
                                             std::shared_ptr<RateLimiter> rate_limiter)
        : txn_kv_(std::move(txn_kv)), rate_limiter_(std::move(rate_limiter)) {}

RateLimitConfigSyncer::~RateLimitConfigSyncer() {
    stop();
}

int RateLimitConfigSyncer::start() {
    if (running_.load()) {
        return 0;
    }
    running_.store(true);
    sync_thread_ = std::make_unique<std::thread>([this] { sync_loop(); });
    pthread_setname_np(sync_thread_->native_handle(), "rate_limit_sync");
    LOG(INFO) << "RateLimitConfigSyncer started";
    return 0;
}

void RateLimitConfigSyncer::stop() {
    if (!running_.load()) {
        return;
    }
    running_.store(false);
    if (sync_thread_ && sync_thread_->joinable()) {
        sync_thread_->join();
    }
    LOG(INFO) << "RateLimitConfigSyncer stopped";
}

void RateLimitConfigSyncer::sync_loop() {
    while (running_.load()) {
        sync_config();
        std::this_thread::sleep_for(
                std::chrono::seconds(config::rate_limit_config_sync_interval_s));
    }
}

void RateLimitConfigSyncer::sync_config() {
    RateLimitConfigPB config;
    if (!load_config(config)) {
        return;
    }

    if (config.version() <= current_version_.load()) {
        return;
    }

    apply_config(config);
    current_version_.store(config.version());
    LOG(INFO) << "Applied rate limit config version=" << config.version();
}

void RateLimitConfigSyncer::apply_config(const RateLimitConfigPB& config) {
    if (config.has_default_qps_limit()) {
        rate_limiter_->set_rate_limit(config.default_qps_limit());
    }

    for (const auto& rpc_limit : config.rpc_qps_limits()) {
        rate_limiter_->set_rate_limit(rpc_limit.qps_limit(), rpc_limit.rpc_name());
    }
}

bool RateLimitConfigSyncer::load_config(RateLimitConfigPB& config) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "Failed to create txn for loading rate limit config";
        return false;
    }

    std::string key(RATE_LIMIT_CONFIG_KEY);
    std::string value;
    err = txn->get(key, &value);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        return false;
    }
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "Failed to get rate limit config from FDB";
        return false;
    }

    if (!config.ParseFromString(value)) {
        LOG(WARNING) << "Failed to parse rate limit config";
        return false;
    }

    return true;
}

bool RateLimitConfigSyncer::save_config(const RateLimitConfigPB& config) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "Failed to create txn for saving rate limit config";
        return false;
    }

    std::string key(RATE_LIMIT_CONFIG_KEY);
    std::string value;
    if (!config.SerializeToString(&value)) {
        LOG(WARNING) << "Failed to serialize rate limit config";
        return false;
    }

    txn->put(key, value);
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "Failed to commit rate limit config";
        return false;
    }

    LOG(INFO) << "Saved rate limit config version=" << config.version();
    return true;
}

} // namespace doris::cloud
