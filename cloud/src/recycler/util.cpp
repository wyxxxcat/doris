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

#include "recycler/util.h"

#include <glog/logging.h>

#include <chrono>
#include <cstdint>
#include <thread>

#include "common/util.h"
#include "meta-service/meta_service_schema.h"
#include "meta-store/keys.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"

namespace doris::cloud {
namespace config {
extern int32_t recycle_job_lease_expired_ms;
} // namespace config

int get_all_instances(TxnKv* txn_kv, std::vector<InstanceInfoPB>& res) {
    InstanceKeyInfo key0_info {""};
    InstanceKeyInfo key1_info {"\xff"}; // instance id are human readable strings
    std::string key0;
    std::string key1;
    instance_key(key0_info, &key0);
    instance_key(key1_info, &key1);

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(INFO) << "failed to init txn, err=" << err;
        return -1;
    }

    std::unique_ptr<RangeGetIterator> it;
    while (it == nullptr /* may be not init */ || it->more()) {
        TxnErrorCode err = txn->get(key0, key1, &it);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to get instance, err=" << err;
            return -1;
        }

        while (it->has_next()) {
            auto [k, v] = it->next();
            if (!it->has_next()) key0 = k;

            InstanceInfoPB instance_info;
            if (!instance_info.ParseFromArray(v.data(), v.size())) {
                LOG(WARNING) << "malformed instance info, key=" << hex(k);
                return -1;
            }
            res.push_back(std::move(instance_info));
        }
        key0.push_back('\x00'); // Update to next smallest key for iteration
    }

    return 0;
}

int prepare_instance_recycle_job(TxnKv* txn_kv, std::string_view key,
                                 const std::string& instance_id, const std::string& ip_port,
                                 int64_t interval_ms) {
    std::string val;
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to create txn";
        return -1;
    }
    err = txn->get(key, &val);
    if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        LOG(WARNING) << "failed to get kv, err=" << err << " key=" << hex(key);
        return -1;
    }
    using namespace std::chrono;
    auto now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    JobRecyclePB job_info;

    auto is_expired = [&]() {
        if (!job_info.ParseFromString(val)) {
            LOG(WARNING) << "failed to parse JobRecyclePB, key=" << hex(key);
            // if failed to parse, just recycle it.
            return true;
        }
        DCHECK(job_info.instance_id() == instance_id);
        if (job_info.status() == JobRecyclePB::BUSY) {
            if (now < job_info.expiration_time_ms()) {
                LOG(INFO) << "job is busy. host=" << job_info.ip_port()
                          << " expiration=" << job_info.expiration_time_ms()
                          << " instance=" << instance_id;
                return false;
            }
        }

        for (const auto& task : job_info.running_task_list()) {
            if (task.status() == JobRecyclePB::BUSY && now < task.expiration_time_ms()) {
                LOG(INFO) << "instance has running distributed task=" << task.task_name()
                          << " host=" << task.ip_port()
                          << " expiration=" << task.expiration_time_ms()
                          << " instance=" << instance_id;
                return false;
            }
        }

        bool finish_expired = now - job_info.last_ctime_ms() > interval_ms;
        if (!finish_expired) {
            LOG(INFO) << "the time since last finished job is too short. host="
                      << job_info.ip_port() << " ctime=" << job_info.last_ctime_ms()
                      << " instance=" << instance_id;
        }

        return finish_expired;
    };

    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND || is_expired()) {
        job_info.set_status(JobRecyclePB::BUSY);
        job_info.set_instance_id(instance_id);
        job_info.set_ip_port(ip_port);
        job_info.set_expiration_time_ms(now + config::recycle_job_lease_expired_ms);
        job_info.clear_running_task_list();
        val = job_info.SerializeAsString();
        txn->put(key, val);
        err = txn->commit();
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to commit, err=" << err << " key=" << hex(key);
            return -1;
        }
        return 0;
    }
    return 1;
}

void finish_instance_recycle_job(TxnKv* txn_kv, std::string_view key,
                                 const std::string& instance_id, const std::string& ip_port,
                                 bool success, int64_t ctime_ms) {
    std::string val;
    int retry_times = 0;
    do {
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to create txn";
            return;
        }
        err = txn->get(key, &val);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to get kv, err=" << err << " key=" << hex(key);
            return;
        }

        using namespace std::chrono;
        auto now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
        JobRecyclePB job_info;
        if (!job_info.ParseFromString(val)) {
            LOG(WARNING) << "failed to parse JobRecyclePB, key=" << hex(key);
            return;
        }
        DCHECK(job_info.instance_id() == instance_id);
        if (job_info.ip_port() != ip_port) {
            LOG(WARNING) << "job is doing at other machine: " << job_info.ip_port()
                         << " key=" << hex(key);
            return;
        }
        if (job_info.status() != JobRecyclePB::BUSY) {
            LOG(WARNING) << "job is not busy, key=" << hex(key);
            return;
        }
        job_info.set_status(JobRecyclePB::IDLE);
        job_info.set_instance_id(instance_id);
        job_info.set_last_finish_time_ms(now);
        job_info.set_last_ctime_ms(ctime_ms);
        if (success) {
            job_info.set_last_success_time_ms(now);
        }
        job_info.clear_running_task_list();
        val = job_info.SerializeAsString();
        txn->put(key, val);
        err = txn->commit();
        if (err == TxnErrorCode::TXN_OK) {
            LOG(INFO) << "succ to commit to finish recycle job, key=" << hex(key);
            return;
        }
        // maybe conflict with the commit of the leased thread
        LOG(WARNING) << "failed to commit to finish recycle job, err=" << err << " key=" << hex(key)
                     << " retry_times=" << retry_times;
    } while (retry_times++ < 3);
    LOG(WARNING) << "finally failed to commit to finish recycle job, key=" << hex(key);
}

int lease_instance_recycle_job(TxnKv* txn_kv, std::string_view key, const std::string& instance_id,
                               const std::string& ip_port) {
    std::string val;
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to create txn";
        return -1;
    }
    err = txn->get(key, &val);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to get kv, err=" << err << " key=" << hex(key);
        return -1;
    }

    using namespace std::chrono;
    auto now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    JobRecyclePB job_info;
    if (!job_info.ParseFromString(val)) {
        LOG(WARNING) << "failed to parse JobRecyclePB, key=" << hex(key);
        return 1;
    }
    DCHECK(job_info.instance_id() == instance_id);
    if (job_info.ip_port() != ip_port) {
        LOG(WARNING) << "job is doing at other machine: " << job_info.ip_port()
                     << " key=" << hex(key);
        return 1;
    }
    if (job_info.status() != JobRecyclePB::BUSY) {
        LOG(WARNING) << "job is not busy, key=" << hex(key);
        return 1;
    }
    job_info.set_expiration_time_ms(now + config::recycle_job_lease_expired_ms);
    val = job_info.SerializeAsString();
    txn->put(key, val);
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to commit, failed to lease recycle job, err=" << err
                     << " key=" << hex(key);
        return -1;
    }
    return 0;
}

static TaskRecyclePB* find_task_in_pb(JobRecyclePB* job_info, std::string_view task_name) {
    for (int i = 0; i < job_info->running_task_list_size(); ++i) {
        auto* task = job_info->mutable_running_task_list(i);
        if (task->task_name() == task_name) {
            return task;
        }
    }
    return nullptr;
}

int prepare_instance_recycle_tasks(TxnKv* txn_kv, std::string_view key,
                                   const std::string& instance_id, const std::string& ip_port,
                                   int64_t interval_ms,
                                   const std::vector<std::string>& candidate_tasks,
                                   std::vector<std::string>* claimed_tasks, int64_t* ctime_ms) {
    claimed_tasks->clear();
    if (candidate_tasks.empty()) {
        return 1;
    }

    constexpr int kMaxClaimRetries = 10;
    for (int retry_times = 0; retry_times < kMaxClaimRetries; ++retry_times) {
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to create txn";
            return -1;
        }

        std::string val;
        err = txn->get(key, &val);
        if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
            LOG(WARNING) << "failed to get kv, err=" << err << " key=" << hex(key);
            return -1;
        }

        using namespace std::chrono;
        auto now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
        JobRecyclePB job_info;
        if (err == TxnErrorCode::TXN_OK) {
            if (!job_info.ParseFromString(val)) {
                LOG(WARNING) << "failed to parse JobRecyclePB, key=" << hex(key);
                job_info.Clear();
            } else {
                if (job_info.has_instance_id()) {
                    DCHECK_EQ(job_info.instance_id(), instance_id);
                }
                if (job_info.status() == JobRecyclePB::BUSY &&
                    now < job_info.expiration_time_ms()) {
                    LOG(INFO) << "instance recycle job is busy in whole-instance mode, host="
                              << job_info.ip_port()
                              << " expiration=" << job_info.expiration_time_ms()
                              << " instance=" << instance_id;
                    return 1;
                }
            }
        }

        std::vector<std::string> to_claim;
        to_claim.reserve(candidate_tasks.size());
        for (const auto& task_name : candidate_tasks) {
            TaskRecyclePB* task_pb = find_task_in_pb(&job_info, task_name);
            if (task_pb != nullptr) {
                bool claimable = task_pb->status() == JobRecyclePB::BUSY
                                         ? (now >= task_pb->expiration_time_ms())
                                         : (now - task_pb->last_ctime_ms() > interval_ms);
                if (!claimable) {
                    continue;
                }
            } else {
                task_pb = job_info.add_running_task_list();
                task_pb->set_task_name(task_name);
            }
            task_pb->set_status(JobRecyclePB::BUSY);
            task_pb->set_ip_port(ip_port);
            task_pb->set_last_ctime_ms(now);
            task_pb->set_expiration_time_ms(now + config::recycle_job_lease_expired_ms);
            to_claim.push_back(task_name);
        }

        if (to_claim.empty()) {
            return 1;
        }

        // Keep instance-level JobRecyclePB.status as IDLE and do not advance its expiration
        job_info.set_instance_id(instance_id);
        job_info.set_status(JobRecyclePB::IDLE);
        val = job_info.SerializeAsString();
        txn->put(key, val);
        err = txn->commit();
        if (err == TxnErrorCode::TXN_OK) {
            *claimed_tasks = std::move(to_claim);
            if (ctime_ms != nullptr) {
                *ctime_ms = now;
            }
            return 0;
        }
        if (err == TxnErrorCode::TXN_CONFLICT) {
            LOG(INFO) << "conflict claiming recycle tasks, key=" << hex(key)
                      << " retry_times=" << retry_times;
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
            continue;
        }
        LOG(WARNING) << "failed to commit claim recycle tasks, err=" << err << " key=" << hex(key);
        return -1;
    }

    LOG(WARNING) << "failed to claim recycle tasks after retries, key=" << hex(key);
    return -1;
}

void finish_instance_recycle_task(TxnKv* txn_kv, std::string_view key,
                                  const std::string& instance_id, const std::string& ip_port,
                                  const std::string& task_name, bool success, int64_t ctime_ms,
                                  bool abort_release) {
    constexpr int kMaxFinishRetries = 10;
    for (int retry_times = 0; retry_times < kMaxFinishRetries; ++retry_times) {
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to create txn";
            return;
        }

        std::string val;
        err = txn->get(key, &val);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to get kv, err=" << err << " key=" << hex(key);
            return;
        }

        JobRecyclePB job_info;
        if (!job_info.ParseFromString(val)) {
            LOG(WARNING) << "failed to parse JobRecyclePB, key=" << hex(key);
            return;
        }
        if (job_info.has_instance_id()) {
            DCHECK_EQ(job_info.instance_id(), instance_id);
        }

        TaskRecyclePB* task_pb = find_task_in_pb(&job_info, task_name);
        if (task_pb == nullptr) {
            LOG(WARNING) << "recycle task not found in running_task_list, task=" << task_name
                         << " key=" << hex(key);
            return;
        }
        if (task_pb->ip_port() != ip_port) {
            LOG(WARNING) << "recycle task is doing at other machine: " << task_pb->ip_port()
                         << " task=" << task_name << " key=" << hex(key);
            return;
        }
        if (task_pb->status() != JobRecyclePB::BUSY) {
            LOG(WARNING) << "recycle task is not busy, task=" << task_name << " key=" << hex(key);
            return;
        }
        if (ctime_ms > 0 && task_pb->last_ctime_ms() != ctime_ms) {
            LOG(WARNING) << "recycle task ctime mismatch, expected=" << ctime_ms
                         << " actual=" << task_pb->last_ctime_ms() << " task=" << task_name
                         << " key=" << hex(key);
            return;
        }

        using namespace std::chrono;
        auto now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
        if (abort_release) {
            task_pb->set_expiration_time_ms(now);
        } else {
            task_pb->set_status(JobRecyclePB::IDLE);
            task_pb->set_last_finish_time_ms(now);
            if (success) {
                task_pb->set_last_success_time_ms(now);
            }
        }

        val = job_info.SerializeAsString();
        txn->put(key, val);
        err = txn->commit();
        if (err == TxnErrorCode::TXN_OK) {
            LOG(INFO) << "succ to commit to " << (abort_release ? "release" : "finish")
                      << " recycle task=" << task_name << " key=" << hex(key);
            return;
        }
        if (err == TxnErrorCode::TXN_CONFLICT) {
            LOG(WARNING) << "conflict finishing recycle task=" << task_name << " key=" << hex(key)
                         << " retry_times=" << retry_times;
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
            continue;
        }
        LOG(WARNING) << "failed to commit to finish recycle task=" << task_name << " err=" << err
                     << " key=" << hex(key);
        return;
    }

    LOG(WARNING) << "finally failed to commit to finish recycle task=" << task_name
                 << " key=" << hex(key);
}

int lease_instance_recycle_tasks(TxnKv* txn_kv, std::string_view key,
                                 const std::string& instance_id, const std::string& ip_port,
                                 const std::vector<std::string>& running_tasks,
                                 std::vector<std::string>* failed_tasks) {
    if (failed_tasks != nullptr) {
        failed_tasks->clear();
    }
    if (running_tasks.empty()) {
        return 0;
    }

    constexpr int kMaxLeaseRetries = 5;
    for (int retry_times = 0; retry_times < kMaxLeaseRetries; ++retry_times) {
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to create txn";
            return -1;
        }

        std::string val;
        err = txn->get(key, &val);
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
            LOG(WARNING) << "recycle job key not found when leasing tasks, key=" << hex(key);
            return 1;
        }
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to get kv, err=" << err << " key=" << hex(key);
            return -1;
        }

        JobRecyclePB job_info;
        if (!job_info.ParseFromString(val)) {
            LOG(WARNING) << "failed to parse JobRecyclePB, key=" << hex(key);
            return 1;
        }
        if (job_info.has_instance_id()) {
            DCHECK_EQ(job_info.instance_id(), instance_id);
        }

        // Fencing: whole-instance mode has acquired the instance lock
        if (job_info.status() == JobRecyclePB::BUSY) {
            LOG(WARNING) << "instance has been locked in whole-instance mode by "
                         << job_info.ip_port()
                         << ", aborting local distributed tasks, instance=" << instance_id;
            return 1;
        }

        using namespace std::chrono;
        auto now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
        std::vector<std::string> current_failed_tasks;
        bool any_renewed = false;
        for (const auto& task_name : running_tasks) {
            TaskRecyclePB* task_pb = find_task_in_pb(&job_info, task_name);
            if (task_pb == nullptr) {
                LOG(WARNING) << "distributed recycle task not found when leasing, task="
                             << task_name << " key=" << hex(key);
                current_failed_tasks.push_back(task_name);
                continue;
            }
            if (task_pb->ip_port() != ip_port) {
                LOG(WARNING) << "distributed recycle task is doing at other machine: "
                             << task_pb->ip_port() << " task=" << task_name << " key=" << hex(key);
                current_failed_tasks.push_back(task_name);
                continue;
            }
            if (task_pb->status() != JobRecyclePB::BUSY) {
                LOG(WARNING) << "distributed recycle task is not busy, task=" << task_name
                             << " key=" << hex(key);
                current_failed_tasks.push_back(task_name);
                continue;
            }
            if (now >= task_pb->expiration_time_ms()) {
                LOG(WARNING) << "distributed recycle task lease expired, task=" << task_name
                             << " now=" << now << " expiration=" << task_pb->expiration_time_ms()
                             << " key=" << hex(key);
                current_failed_tasks.push_back(task_name);
                continue;
            }
            task_pb->set_expiration_time_ms(now + config::recycle_job_lease_expired_ms);
            any_renewed = true;
        }

        if (!any_renewed) {
            if (failed_tasks != nullptr) {
                *failed_tasks = std::move(current_failed_tasks);
            }
            return 0;
        }

        val = job_info.SerializeAsString();
        txn->put(key, val);
        err = txn->commit();
        if (err == TxnErrorCode::TXN_OK) {
            if (failed_tasks != nullptr) {
                *failed_tasks = std::move(current_failed_tasks);
            }
            return 0;
        }
        if (err == TxnErrorCode::TXN_CONFLICT) {
            LOG(WARNING) << "conflict leasing recycle tasks, key=" << hex(key)
                         << " retry_times=" << retry_times;
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
            continue;
        }
        LOG(WARNING) << "failed to commit, failed to lease recycle tasks, err=" << err
                     << " key=" << hex(key);
        return -1;
    }

    LOG(WARNING) << "finally failed to lease recycle tasks after retries, key=" << hex(key);
    return -1;
}

// ret: 0: success, 1: tablet not found, -1: failed
int get_tablet_idx(TxnKv* txn_kv, const std::string& instance_id, int64_t tablet_id,
                   TabletIndexPB& tablet_idx) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to create txn";
        return -1;
    }

    std::string key, val;
    meta_tablet_idx_key({instance_id, tablet_id}, &key);
    err = txn->get(key, &val);
    if (err != TxnErrorCode::TXN_OK) {
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
            LOG(INFO) << "tablet not found, tablet_id=" << tablet_id << " key=" << hex(key);
            return 1;
        }
        LOG(WARNING) << fmt::format("failed to get tablet_idx, err={} tablet_id={} key={}", err,
                                    tablet_id, hex(key));
        return -1;
    }
    if (!tablet_idx.ParseFromString(val)) [[unlikely]] {
        LOG(WARNING) << fmt::format("malformed tablet index value, tablet_id={} key={}", tablet_id,
                                    hex(key));
        return -1;
    }
    if (tablet_id != tablet_idx.tablet_id()) [[unlikely]] {
        LOG(WARNING) << "unexpected error given_tablet_id=" << tablet_id
                     << " idx_pb_tablet_id=" << tablet_idx.tablet_id() << " key=" << hex(key);
        return -1;
    }
    return 0;
}

int get_tablet_meta(TxnKv* txn_kv, const std::string& instance_id, int64_t tablet_id,
                    TabletMetaCloudPB& tablet_meta) {
    TabletIndexPB tablet_idx;
    int ret = get_tablet_idx(txn_kv, instance_id, tablet_id, tablet_idx);
    if (ret < 0) {
        return ret;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to create txn";
        return -1;
    }

    std::string key, val;
    meta_tablet_key({instance_id, tablet_idx.table_id(), tablet_idx.index_id(),
                     tablet_idx.partition_id(), tablet_id},
                    &key);
    err = txn->get(key, &val);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << fmt::format(
                "failed to get tablet, err={}, table_id={}, index_id={}, partition_id={}, "
                "tablet_id={} key={}",
                err, tablet_idx.table_id(), tablet_idx.index_id(), tablet_idx.partition_id(),
                tablet_id, hex(key));
        return -1;
    }
    if (!tablet_meta.ParseFromString(val)) [[unlikely]] {
        LOG(WARNING) << fmt::format("malformed tablet meta, tablet_id={} key={}", tablet_id,
                                    hex(key));
        return -1;
    }
    return 0;
}
} // namespace doris::cloud
