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

#include <fmt/core.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <utility>

#include "common/bvars.h"
#include "common/config.h"
#include "common/simple_thread_pool.h"
#include "meta-service/meta_service_schema.h"
#include "meta-service/txn_lazy_committer.h"
#include "meta-store/blob_message.h"
#include "meta-store/keys.h"
#include "meta-store/mem_txn_kv.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "recycler/recycler.h"
#include "recycler/s3_accessor.h"

namespace doris::cloud {
namespace {

const std::string kBenchmarkInstanceId = "recycler_benchmark_instance";
const std::string kBenchmarkResourceId = "recycler_benchmark_resource";

std::string get_env(const char* name) {
    const char* value = std::getenv(name);
    return value == nullptr ? "" : value;
}

std::string get_env_with_fallback(const char* primary, const char* fallback) {
    const char* value = std::getenv(primary);
    return value == nullptr ? get_env(fallback) : std::string(value);
}

struct BenchmarkS3Config {
    bool enabled = false;
    std::string access_key;
    std::string secret_key;
    std::string role_arn;
    std::string external_id;
    std::string endpoint;
    std::string provider;
    std::string bucket;
    std::string region;
    std::string prefix;
};

BenchmarkS3Config load_benchmark_s3_config() {
    BenchmarkS3Config config;
    config.enabled = get_env("ENABLE_S3_CLIENT") == "1";
    if (!config.enabled) {
        return config;
    }

    config.access_key = get_env("S3_AK");
    config.secret_key = get_env("S3_SK");
    config.role_arn = get_env("AWS_ROLE_ARN");
    config.external_id = get_env("AWS_EXTERNAL_ID");
    config.endpoint = get_env_with_fallback("S3_ENDPOINT", "AWS_ENDPOINT");
    config.provider = get_env("S3_PROVIDER");
    config.bucket = get_env_with_fallback("S3_BUCKET", "AWS_BUCKET");
    config.region = get_env_with_fallback("S3_REGION", "AWS_REGION");
    config.prefix = get_env_with_fallback("S3_PREFIX", "AWS_PREFIX");
    return config;
}

void set_obj_store_provider(const std::string& provider, ObjectStoreInfoPB* obj_info) {
    if (provider == "AZURE") {
        obj_info->set_provider(ObjectStoreInfoPB_Provider_AZURE);
    } else if (provider == "GCS") {
        obj_info->set_provider(ObjectStoreInfoPB_Provider_GCP);
    } else {
        obj_info->set_provider(ObjectStoreInfoPB_Provider_S3);
    }
}

// Number of recyclable rowsets seeded per branch.
constexpr int64_t kRowsetsPerBranch = 50000;
// Commit the seeded recycle rowset KVs in batches to keep each txn small.
constexpr int64_t kSeedCommitBatch = 2000;
constexpr int64_t kBenchmarkIndexId = 20000;
constexpr int32_t kBenchmarkSchemaVersion = 1;

doris::TabletSchemaCloudPB make_benchmark_schema() {
    doris::TabletSchemaCloudPB schema;
    schema.set_schema_version(kBenchmarkSchemaVersion);
    schema.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V1);
    auto* index = schema.add_index();
    index->set_index_id(1);
    index->set_index_type(IndexType::INVERTED);
    return schema;
}

int put_benchmark_schema(TxnKv* txn_kv, const std::string& instance_id) {
    std::unique_ptr<Transaction> txn;
    if (txn_kv->create_txn(&txn) != TxnErrorCode::TXN_OK) {
        return -1;
    }
    std::string schema_key;
    meta_schema_key({instance_id, kBenchmarkIndexId, kBenchmarkSchemaVersion}, &schema_key);
    auto schema = make_benchmark_schema();
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg;
    put_schema_kv(code, msg, txn.get(), schema_key, schema);
    if (code != MetaServiceCode::OK) {
        return -1;
    }
    return txn->commit() == TxnErrorCode::TXN_OK ? 0 : -1;
}

int remove_benchmark_schema(TxnKv* txn_kv, const std::string& instance_id) {
    std::unique_ptr<Transaction> txn;
    if (txn_kv->create_txn(&txn) != TxnErrorCode::TXN_OK) {
        return -1;
    }
    std::string schema_key;
    meta_schema_key({instance_id, kBenchmarkIndexId, kBenchmarkSchemaVersion}, &schema_key);
    ValueBuf schema_value;
    if (blob_get(txn.get(), schema_key, &schema_value) != TxnErrorCode::TXN_OK) {
        return -1;
    }
    schema_value.remove(txn.get());
    return txn->commit() == TxnErrorCode::TXN_OK ? 0 : -1;
}

// Each value maps to one recyclable branch inside
// InstanceRecycler::recycle_rowsets()::handle_rowset_kv. Seeding a branch plus
// the matching config flags (see configure_flags_for_branch) drives the scan
// down exactly that code path.
enum class RecycleRowsetBranch {
    // Old-version RecycleRowsetPB (no `type`) whose resource_id is empty: the
    // recycler removes the KV only, without touching object storage.
    kLegacyEmptyResource,
    // Old-version RecycleRowsetPB with a real resource_id: recycled through the
    // single-rowset delete_rowset_data_by_prefix path.
    kLegacyWithResource,
    // New PREPARE rowset recycled directly (mark/abort flags both off).
    kPrepareDirect,
    // New PREPARE rowset queued to be marked as recycled first
    // (enable_mark_delete_rowset_before_recycle = true).
    kPrepareMark,
    // New PREPARE rowset carrying a load_id that triggers the abort-txn/job path
    // (enable_abort_txn_and_job_for_delete_rowset_before_recycle = true).
    kPrepareAbort,
    // New COMPACT/DROP rowset with segments: recycled via the batched
    // delete_rowset_data path.
    kCompactedWithData,
    // New COMPACT/DROP rowset without segments: treated as an empty rowset and
    // removed by KV delete only.
    kCompactedEmpty,
};

// Build one RowsetMetaCloudPB for the given branch. Kept intentionally minimal
// and self-contained so this benchmark does not depend on recycler_test.cpp.
doris::RowsetMetaCloudPB make_rowset_meta(RecycleRowsetBranch branch, int64_t tablet_id,
                                          const std::string& rowset_id) {
    doris::RowsetMetaCloudPB meta;
    meta.set_rowset_id(0); // deprecated but required
    meta.set_rowset_id_v2(rowset_id);
    meta.set_tablet_id(tablet_id);
    meta.set_index_id(kBenchmarkIndexId);
    meta.set_schema_version(kBenchmarkSchemaVersion);
    meta.mutable_tablet_schema()->CopyFrom(make_benchmark_schema());
    meta.set_start_version(2);
    meta.set_end_version(2);
    meta.set_data_disk_size(1024);
    meta.set_index_disk_size(512);
    meta.set_total_disk_size(1536);
    switch (branch) {
    case RecycleRowsetBranch::kCompactedEmpty:
        meta.set_num_segments(0);
        break;
    default:
        meta.set_num_segments(1);
        break;
    }
    // Only the branches that reach delete_rowset_data need a resolvable resource.
    if (branch != RecycleRowsetBranch::kLegacyEmptyResource) {
        meta.set_resource_id(kBenchmarkResourceId);
    }
    if (branch == RecycleRowsetBranch::kPrepareAbort) {
        // A load_id makes make_related_txn_or_job_abort_task emit a TXN abort
        // task; end_version != 1 is required to enter the abort branch.
        meta.mutable_load_id()->set_hi(tablet_id);
        meta.mutable_load_id()->set_lo(1);
        meta.set_txn_id(tablet_id);
    }
    return meta;
}

// Wrap a RowsetMetaCloudPB into a RecycleRowsetPB shaped for the branch.
RecycleRowsetPB make_recycle_rowset(RecycleRowsetBranch branch,
                                    const doris::RowsetMetaCloudPB& meta) {
    RecycleRowsetPB pb;
    pb.set_creation_time(1); // long expired once retention is 0 / immediate recycle
    pb.set_expiration(1);
    switch (branch) {
    case RecycleRowsetBranch::kLegacyEmptyResource:
        // Old-version layout: no `type`, resource_id left empty on purpose.
        pb.set_tablet_id(meta.tablet_id());
        pb.set_resource_id("");
        break;
    case RecycleRowsetBranch::kLegacyWithResource:
        // Old-version layout: no `type`, resource_id populated.
        pb.set_tablet_id(meta.tablet_id());
        pb.set_resource_id(meta.resource_id());
        break;
    case RecycleRowsetBranch::kPrepareDirect:
    case RecycleRowsetBranch::kPrepareMark:
    case RecycleRowsetBranch::kPrepareAbort:
        pb.set_type(RecycleRowsetPB::PREPARE);
        pb.mutable_rowset_meta()->CopyFrom(meta);
        break;
    case RecycleRowsetBranch::kCompactedWithData:
    case RecycleRowsetBranch::kCompactedEmpty:
        pb.set_type(RecycleRowsetPB::COMPACT);
        pb.mutable_rowset_meta()->CopyFrom(meta);
        if (branch == RecycleRowsetBranch::kCompactedWithData) {
            // Match production rowsets whose schema is stored separately in the schema KV.
            pb.mutable_rowset_meta()->clear_tablet_schema();
        }
        break;
    }
    return pb;
}

// Seed `count` recycle rowset KVs for one branch. Every rowset gets a distinct
// tablet_id so the per-tablet recycle batch limit never truncates the workload.
int seed_recycle_rowsets(TxnKv* txn_kv, const std::string& instance_id, RecycleRowsetBranch branch,
                         int64_t count, int64_t tablet_id_base, bool write_schema_kv = true) {
    if (write_schema_kv && put_benchmark_schema(txn_kv, instance_id) != 0) {
        return -1;
    }
    std::unique_ptr<Transaction> txn;
    for (int64_t i = 0; i < count; ++i) {
        if (i % kSeedCommitBatch == 0) {
            if (txn) {
                if (txn->commit() != TxnErrorCode::TXN_OK) {
                    return -1;
                }
            }
            if (txn_kv->create_txn(&txn) != TxnErrorCode::TXN_OK) {
                return -1;
            }
        }
        int64_t tablet_id = tablet_id_base + i;
        std::string rowset_id = fmt::format("{:018d}", i);
        auto meta = make_rowset_meta(branch, tablet_id, rowset_id);
        auto pb = make_recycle_rowset(branch, meta);

        std::string key;
        recycle_rowset_key({instance_id, tablet_id, rowset_id}, &key);
        std::string val;
        pb.SerializeToString(&val);
        txn->put(key, val);
    }
    if (txn && txn->commit() != TxnErrorCode::TXN_OK) {
        return -1;
    }
    return 0;
}

class RecyclerBenchmarkTest : public ::testing::Test {
protected:
    struct RecycleMetrics {
        int64_t num = 0;
        int64_t bytes = 0;
    };

    struct BenchmarkResult {
        RecycleMetrics metrics;
        double elapsed_ms = 0;
    };

    using RecycleFunction = int (InstanceRecycler::*)();

    static const std::map<std::string, RecycleFunction>& recycle_functions() {
        static const std::map<std::string, RecycleFunction> functions = {
                {"recycle_cluster_snapshots", &InstanceRecycler::recycle_cluster_snapshots},
                {"recycle_operation_logs", &InstanceRecycler::recycle_operation_logs},
                {"recycle_indexes", &InstanceRecycler::recycle_indexes},
                {"recycle_partitions", &InstanceRecycler::recycle_partitions},
                {"recycle_tmp_rowsets", &InstanceRecycler::recycle_tmp_rowsets},
                {"recycle_rowsets", &InstanceRecycler::recycle_rowsets},
                {"recycle_packed_files", &InstanceRecycler::recycle_packed_files},
                {"abort_timeout_txn", &InstanceRecycler::abort_timeout_txn},
                {"recycle_expired_txn_label", &InstanceRecycler::recycle_expired_txn_label},
                {"recycle_copy_jobs", &InstanceRecycler::recycle_copy_jobs},
                {"recycle_stage", &InstanceRecycler::recycle_stage},
                {"recycle_expired_stage_objects", &InstanceRecycler::recycle_expired_stage_objects},
                {"recycle_versions", &InstanceRecycler::recycle_versions},
                {"recycle_restore_jobs", &InstanceRecycler::recycle_restore_jobs},
        };
        return functions;
    }

    void SetUp() override {
        old_force_immediate_recycle_ = config::force_immediate_recycle;
        old_retention_seconds_ = config::retention_seconds;

        config::force_immediate_recycle = true;
        config::retention_seconds = 0;

        txn_kv_ = std::make_shared<MemTxnKv>();
        ASSERT_EQ(txn_kv_->init(), 0);

        instance_.set_instance_id(std::string(kBenchmarkInstanceId));
        auto* obj_info = instance_.add_obj_info();
        obj_info->set_id(kBenchmarkResourceId);
        const auto s3_config = load_benchmark_s3_config();
        ASSERT_NO_FATAL_FAILURE(configure_obj_info(s3_config, obj_info));

        s3_producer_pool_ = std::make_shared<SimpleThreadPool>(
                config::recycle_pool_parallelism, "recycler_benchmark_s3_producer_pool");
        recycle_tablet_pool_ = std::make_shared<SimpleThreadPool>(
                config::recycle_pool_parallelism, "recycler_benchmark_recycle_tablet_pool");
        group_recycle_function_pool_ = std::make_shared<SimpleThreadPool>(
                config::recycle_pool_parallelism, "recycler_benchmark_group_recycle_function_pool");
        ASSERT_EQ(s3_producer_pool_->start(), 0);
        ASSERT_EQ(recycle_tablet_pool_->start(), 0);
        ASSERT_EQ(group_recycle_function_pool_->start(), 0);

        thread_pool_group_ = RecyclerThreadPoolGroup(s3_producer_pool_, recycle_tablet_pool_,
                                                     group_recycle_function_pool_);
        txn_lazy_committer_ = std::make_shared<TxnLazyCommitter>(txn_kv_);
        recycler_ = std::make_unique<InstanceRecycler>(txn_kv_, instance_, thread_pool_group_,
                                                       txn_lazy_committer_);

        if (s3_config.enabled) {
            auto s3_conf = S3Conf::from_obj_store_info(*obj_info);
            ASSERT_TRUE(s3_conf.has_value());

            std::shared_ptr<S3Accessor> accessor;
            ASSERT_EQ(S3Accessor::create(std::move(*s3_conf), &accessor), 0);
            recycler_->TEST_add_accessor(kBenchmarkResourceId, std::move(accessor));
        }
        ASSERT_EQ(recycler_->init(), 0);
    }

    static void configure_obj_info(const BenchmarkS3Config& s3_config,
                                   ObjectStoreInfoPB* obj_info) {
        if (!s3_config.enabled) {
            obj_info->set_prefix(kBenchmarkResourceId);
            return;
        }
        ASSERT_FALSE(s3_config.endpoint.empty());
        ASSERT_FALSE(s3_config.region.empty());
        ASSERT_FALSE(s3_config.bucket.empty());
        ASSERT_FALSE(s3_config.prefix.empty());
        ASSERT_TRUE((s3_config.access_key.empty() && s3_config.secret_key.empty()) ||
                    (!s3_config.access_key.empty() && !s3_config.secret_key.empty()));

        obj_info->set_ak(s3_config.access_key);
        obj_info->set_sk(s3_config.secret_key);
        obj_info->set_endpoint(s3_config.endpoint);
        obj_info->set_region(s3_config.region);
        obj_info->set_bucket(s3_config.bucket);
        obj_info->set_prefix(s3_config.prefix);
        set_obj_store_provider(s3_config.provider, obj_info);
        if (s3_config.access_key.empty()) {
            obj_info->set_role_arn(s3_config.role_arn);
            obj_info->set_external_id(s3_config.external_id);
            obj_info->set_cred_provider_type(CredProviderTypePB::INSTANCE_PROFILE);
        }
    }

    void TearDown() override {
        for (auto& [operation_type, result] : benchmark_results_) {
            result.metrics = read_recycle_metrics(operation_type);
            std::cout << "recycler benchmark: operation=" << operation_type
                      << ", elapsed_ms=" << result.elapsed_ms
                      << ", recycled_num=" << result.metrics.num
                      << ", recycled_bytes=" << result.metrics.bytes << std::endl;
        }

        recycler_.reset();
        txn_lazy_committer_.reset();
        thread_pool_group_ = {};
        if (group_recycle_function_pool_) {
            ASSERT_EQ(group_recycle_function_pool_->stop(), 0);
        }
        if (recycle_tablet_pool_) {
            ASSERT_EQ(recycle_tablet_pool_->stop(), 0);
        }
        if (s3_producer_pool_) {
            ASSERT_EQ(s3_producer_pool_->stop(), 0);
        }
        group_recycle_function_pool_.reset();
        recycle_tablet_pool_.reset();
        s3_producer_pool_.reset();
        txn_kv_.reset();

        config::force_immediate_recycle = old_force_immediate_recycle_;
        config::retention_seconds = old_retention_seconds_;
    }

    RecycleMetrics read_recycle_metrics(const std::string& operation_type) const {
        return {.num = g_bvar_recycler_instance_recycle_total_num_since_started.get(
                        {kBenchmarkInstanceId, operation_type}),
                .bytes = g_bvar_recycler_instance_recycle_total_bytes_since_started.get(
                        {kBenchmarkInstanceId, operation_type})};
    }

    void measure(const std::string& operation_type) {
        const auto function_it = recycle_functions().find(operation_type);
        ASSERT_NE(function_it, recycle_functions().end())
                << "unknown recycler operation: " << operation_type;

        const auto start = std::chrono::steady_clock::now();
        ASSERT_EQ((recycler_.get()->*(function_it->second))(), 0)
                << "recycler operation failed: " << operation_type;
        const auto elapsed =
                std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start);
        benchmark_results_[operation_type].elapsed_ms = elapsed.count();
    }

    int64_t count_recycle_rowsets() {
        std::string begin = recycle_key_prefix(kBenchmarkInstanceId);
        std::string end = begin;
        end.back()++;
        std::unique_ptr<Transaction> txn;
        if (txn_kv_->create_txn(&txn) != TxnErrorCode::TXN_OK) {
            return -1;
        }
        int64_t count = 0;
        std::unique_ptr<RangeGetIterator> it;
        do {
            if (txn->get(begin, end, &it) != TxnErrorCode::TXN_OK) {
                return -1;
            }
            count += it->size();
            begin = it->next_begin_key();
        } while (it->more());
        return count;
    }

    std::shared_ptr<MemTxnKv> txn_kv_;
    InstanceInfoPB instance_;
    RecyclerThreadPoolGroup thread_pool_group_;
    std::shared_ptr<SimpleThreadPool> s3_producer_pool_;
    std::shared_ptr<SimpleThreadPool> recycle_tablet_pool_;
    std::shared_ptr<SimpleThreadPool> group_recycle_function_pool_;
    std::shared_ptr<TxnLazyCommitter> txn_lazy_committer_;
    std::unique_ptr<InstanceRecycler> recycler_;
    std::map<std::string, BenchmarkResult> benchmark_results_;

    bool old_force_immediate_recycle_ = false;
    int64_t old_retention_seconds_ = 0;
};

// Non-overlapping tablet id ranges per branch so a single recycle_rowsets() run
// can cover several branches at once without id collisions.
constexpr int64_t kTabletBase = 1'000'000;
constexpr int64_t kTabletStride = 1'000'000'000LL;

int64_t tablet_base_for(RecycleRowsetBranch branch) {
    return kTabletBase + static_cast<int64_t>(branch) * kTabletStride;
}

TEST_F(RecyclerBenchmarkTest, RecycleRowsetsLegacyEmptyResource) {
    ASSERT_EQ(seed_recycle_rowsets(txn_kv_.get(), kBenchmarkInstanceId,
                                   RecycleRowsetBranch::kLegacyEmptyResource, kRowsetsPerBranch,
                                   tablet_base_for(RecycleRowsetBranch::kLegacyEmptyResource)),
              0);
    measure("recycle_rowsets");
    ASSERT_EQ(count_recycle_rowsets(), 0);
}

TEST_F(RecyclerBenchmarkTest, RecycleRowsetsLegacyWithResource) {
    ASSERT_EQ(seed_recycle_rowsets(txn_kv_.get(), kBenchmarkInstanceId,
                                   RecycleRowsetBranch::kLegacyWithResource, kRowsetsPerBranch,
                                   tablet_base_for(RecycleRowsetBranch::kLegacyWithResource)),
              0);
    measure("recycle_rowsets");
    ASSERT_EQ(count_recycle_rowsets(), 0);
}

TEST_F(RecyclerBenchmarkTest, RecycleRowsetsPrepareDirect) {
    ASSERT_EQ(seed_recycle_rowsets(txn_kv_.get(), kBenchmarkInstanceId,
                                   RecycleRowsetBranch::kPrepareDirect, kRowsetsPerBranch,
                                   tablet_base_for(RecycleRowsetBranch::kPrepareDirect)),
              0);
    measure("recycle_rowsets");
    ASSERT_EQ(count_recycle_rowsets(), 0);
}

TEST_F(RecyclerBenchmarkTest, RecycleRowsetsPrepareMark) {
    ASSERT_EQ(seed_recycle_rowsets(txn_kv_.get(), kBenchmarkInstanceId,
                                   RecycleRowsetBranch::kPrepareMark, kRowsetsPerBranch,
                                   tablet_base_for(RecycleRowsetBranch::kPrepareMark)),
              0);
    // First pass only marks the rowsets as recycled; the second pass deletes them.
    measure("recycle_rowsets");
    if (config::enable_mark_delete_rowset_before_recycle) {
        ASSERT_GT(count_recycle_rowsets(), 0);
        measure("recycle_rowsets");
    }
    ASSERT_EQ(count_recycle_rowsets(), 0);
}

TEST_F(RecyclerBenchmarkTest, RecycleRowsetsPrepareAbort) {
    ASSERT_EQ(seed_recycle_rowsets(txn_kv_.get(), kBenchmarkInstanceId,
                                   RecycleRowsetBranch::kPrepareAbort, kRowsetsPerBranch,
                                   tablet_base_for(RecycleRowsetBranch::kPrepareAbort)),
              0);
    measure("recycle_rowsets");
    ASSERT_EQ(count_recycle_rowsets(), 0);
}

TEST_F(RecyclerBenchmarkTest, RecycleRowsetsCompactedWithData) {
    ASSERT_EQ(seed_recycle_rowsets(txn_kv_.get(), kBenchmarkInstanceId,
                                   RecycleRowsetBranch::kCompactedWithData, kRowsetsPerBranch,
                                   tablet_base_for(RecycleRowsetBranch::kCompactedWithData)),
              0);
    ASSERT_EQ(remove_benchmark_schema(txn_kv_.get(), kBenchmarkInstanceId), 0);
    measure("recycle_rowsets");
    ASSERT_EQ(seed_recycle_rowsets(txn_kv_.get(), kBenchmarkInstanceId,
                                   RecycleRowsetBranch::kCompactedWithData, 1,
                                   tablet_base_for(RecycleRowsetBranch::kCompactedWithData), true),
              0);
    // Retry after the first cleanup to exercise the missing schema path again.
    measure("recycle_rowsets");
    ASSERT_EQ(count_recycle_rowsets(), 0);
}

TEST_F(RecyclerBenchmarkTest, RecycleRowsetsCompactedEmpty) {
    ASSERT_EQ(seed_recycle_rowsets(txn_kv_.get(), kBenchmarkInstanceId,
                                   RecycleRowsetBranch::kCompactedEmpty, kRowsetsPerBranch,
                                   tablet_base_for(RecycleRowsetBranch::kCompactedEmpty)),
              0);
    measure("recycle_rowsets");
    ASSERT_EQ(count_recycle_rowsets(), 0);
}

// Seeds every branch and recycles them together, exercising all recyclable
// paths in a single recycle_rowsets() run.
TEST_F(RecyclerBenchmarkTest, RecycleRowsetsAllBranches) {
    const RecycleRowsetBranch branches[] = {
            RecycleRowsetBranch::kLegacyEmptyResource, RecycleRowsetBranch::kLegacyWithResource,
            RecycleRowsetBranch::kPrepareDirect,       RecycleRowsetBranch::kPrepareMark,
            RecycleRowsetBranch::kPrepareAbort,        RecycleRowsetBranch::kCompactedWithData,
            RecycleRowsetBranch::kCompactedEmpty,
    };
    for (auto branch : branches) {
        ASSERT_EQ(seed_recycle_rowsets(txn_kv_.get(), kBenchmarkInstanceId, branch,
                                       kRowsetsPerBranch, tablet_base_for(branch)),
                  0);
    }
    // Two passes so the mark-first PREPARE rowsets also get deleted.
    measure("recycle_rowsets");
    if (config::enable_mark_delete_rowset_before_recycle) {
        ASSERT_GT(count_recycle_rowsets(), 0);
        measure("recycle_rowsets");
    }
    ASSERT_EQ(count_recycle_rowsets(), 0);
}

} // namespace
} // namespace doris::cloud
