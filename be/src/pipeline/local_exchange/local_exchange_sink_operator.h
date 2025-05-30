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

#include "pipeline/exec/operator.h"

namespace doris::vectorized {
class PartitionerBase;
}

namespace doris::pipeline {

class ExchangerBase;
class ShuffleExchanger;
class PassthroughExchanger;
class BroadcastExchanger;
class PassToOneExchanger;
class LocalExchangeSinkOperatorX;
class LocalExchangeSinkLocalState final : public PipelineXSinkLocalState<LocalExchangeSharedState> {
public:
    using Base = PipelineXSinkLocalState<LocalExchangeSharedState>;
    ENABLE_FACTORY_CREATOR(LocalExchangeSinkLocalState);

    LocalExchangeSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : Base(parent, state) {}
    ~LocalExchangeSinkLocalState() override;

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status open(RuntimeState* state) override;
    std::string debug_string(int indentation_level) const override;
    std::vector<Dependency*> dependencies() const override;
    Status close(RuntimeState* state, Status exec_status) override;

private:
    friend class LocalExchangeSinkOperatorX;
    friend class ShuffleExchanger;
    friend class BucketShuffleExchanger;
    friend class PassthroughExchanger;
    friend class BroadcastExchanger;
    friend class PassToOneExchanger;
    friend class AdaptivePassthroughExchanger;
    template <typename BlockType>
    friend class Exchanger;

    ExchangerBase* _exchanger = nullptr;

    // Used by shuffle exchanger
    RuntimeProfile::Counter* _compute_hash_value_timer = nullptr;
    RuntimeProfile::Counter* _distribute_timer = nullptr;
    std::unique_ptr<vectorized::PartitionerBase> _partitioner = nullptr;

    // Used by random passthrough exchanger
    int _channel_id = 0;
};

// A single 32-bit division on a recent x64 processor has a throughput of one instruction every six cycles with a latency of 26 cycles.
// In contrast, a multiplication has a throughput of one instruction every cycle and a latency of 3 cycles.
// So we prefer to this algorithm instead of modulo.
// Reference: https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
struct LocalExchangeChannelIds {
    static constexpr auto SHIFT_BITS = 32;
    uint32_t operator()(uint32_t l, uint32_t r) {
        return ((uint64_t)l * (uint64_t)r) >> SHIFT_BITS;
    }
};

class LocalExchangeSinkOperatorX final : public DataSinkOperatorX<LocalExchangeSinkLocalState> {
public:
    using Base = DataSinkOperatorX<LocalExchangeSinkLocalState>;
    LocalExchangeSinkOperatorX(int sink_id, int dest_id, int num_partitions,
                               const std::vector<TExpr>& texprs,
                               const std::map<int, int>& bucket_seq_to_instance_idx)
            : Base(sink_id, dest_id, dest_id),
              _num_partitions(num_partitions),
              _texprs(texprs),
              _partitioned_exprs_num(texprs.size()),
              _shuffle_idx_to_instance_idx(bucket_seq_to_instance_idx) {}
#ifdef BE_TEST
    LocalExchangeSinkOperatorX(const std::vector<TExpr>& texprs,
                               const std::map<int, int>& bucket_seq_to_instance_idx)
            : Base(),
              _num_partitions(0),
              _texprs(texprs),
              _partitioned_exprs_num(texprs.size()),
              _shuffle_idx_to_instance_idx(bucket_seq_to_instance_idx) {}
#endif

    Status init(const TPlanNode& tnode, RuntimeState* state) override {
        return Status::InternalError("{} should not init with TPlanNode", Base::_name);
    }

    Status init(const TDataSink& tsink) override {
        return Status::InternalError("{} should not init with TPlanNode", Base::_name);
    }

    Status init(ExchangeType type, const int num_buckets, const bool use_global_hash_shuffle,
                const std::map<int, int>& shuffle_idx_to_instance_idx) override;

    Status prepare(RuntimeState* state) override;

    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos) override;

    void set_low_memory_mode(RuntimeState* state) override {
        auto& local_state = get_local_state(state);
        SCOPED_TIMER(local_state.exec_time_counter());
        local_state._shared_state->set_low_memory_mode(state);
        local_state._exchanger->set_low_memory_mode();
    }

private:
    friend class LocalExchangeSinkLocalState;
    friend class ShuffleExchanger;
    ExchangeType _type;
    const int _num_partitions;
    const std::vector<TExpr>& _texprs;
    const size_t _partitioned_exprs_num;
    std::unique_ptr<vectorized::PartitionerBase> _partitioner;
    std::map<int, int> _shuffle_idx_to_instance_idx;
    bool _use_global_shuffle = false;
};

} // namespace doris::pipeline
