// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <atomic>
#include <chrono>
#include <mutex>
#include <optional>
#include <unordered_map>

#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/pipeline/stream_epoch_manager.h"
#include "exec/spill/query_spill_manager.h"
#include "gen_cpp/InternalService_types.h" // for TQueryOptions
#include "gen_cpp/Types_types.h"           // for TUniqueId
#include "gen_cpp/internal_service.pb.h"
#include "runtime/profile_report_worker.h"
#include "runtime/query_statistics.h"
#include "runtime/runtime_state.h"
#include "util/debug/query_trace.h"
#include "util/hash.h"
#include "util/hash_util.hpp"
#include "util/spinlock.h"
#include "util/time.h"

namespace starrocks {

class StreamEpochManager;

namespace pipeline {

using std::chrono::seconds;
using std::chrono::milliseconds;
using std::chrono::steady_clock;
using std::chrono::duration_cast;

struct ConnectorScanOperatorMemShareArbitrator;

// The context for all fragment of one query in one BE
class QueryContext : public std::enable_shared_from_this<QueryContext> {
public:
    QueryContext();
    ~QueryContext() noexcept;
    void set_exec_env(ExecEnv* exec_env) { _exec_env = exec_env; }
    void set_query_id(const TUniqueId& query_id) { _query_id = query_id; }
    TUniqueId query_id() const { return _query_id; }
    int64_t lifetime() { return _lifetime_sw.elapsed_time(); }
    void set_total_fragments(size_t total_fragments) { _total_fragments = total_fragments; }

    void increment_num_fragments() {
        _num_fragments.fetch_add(1);
        _num_active_fragments.fetch_add(1);
    }

    void rollback_inc_fragments() {
        _num_fragments.fetch_sub(1);
        _num_active_fragments.fetch_sub(1);
    }

    void count_down_fragments();
    int num_active_fragments() const { return _num_active_fragments.load(); }
    bool has_no_active_instances() { return _num_active_fragments.load() == 0; }

    void set_delivery_expire_seconds(int expire_seconds) { _delivery_expire_seconds = seconds(expire_seconds); }
    void set_query_expire_seconds(int expire_seconds) { _query_expire_seconds = seconds(expire_seconds); }
    inline int get_query_expire_seconds() const { return _query_expire_seconds.count(); }
    // now time point pass by deadline point.
    bool is_delivery_expired() const {
        auto now = duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
        return now > _delivery_deadline || _cancelled_by_fe;
    }
    bool is_query_expired() const {
        auto now = duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
        return now > _query_deadline;
    }

    bool is_cancelled() const { return _is_cancelled; }

    Status get_cancelled_status() const {
        auto* status = _cancelled_status.load();
        return status == nullptr ? Status::Cancelled("Query has been cancelled") : *status;
    }

    bool is_dead() const {
        return _num_active_fragments == 0 && (_num_fragments == _total_fragments || _cancelled_by_fe);
    }
    // add expired seconds to deadline
    void extend_delivery_lifetime() {
        _delivery_deadline =
                duration_cast<milliseconds>(steady_clock::now().time_since_epoch() + _delivery_expire_seconds).count();
    }
    void extend_query_lifetime() {
        _query_deadline =
                duration_cast<milliseconds>(steady_clock::now().time_since_epoch() + _query_expire_seconds).count();
    }
    void set_enable_pipeline_level_shuffle(bool flag) { _enable_pipeline_level_shuffle = flag; }
    bool enable_pipeline_level_shuffle() { return _enable_pipeline_level_shuffle; }
    void set_enable_profile() { _enable_profile = true; }
    bool get_enable_profile_flag() { return _enable_profile; }
    bool enable_profile() {
        if (_enable_profile) {
            return true;
        }
        if (_big_query_profile_threshold_ns <= 0) {
            return false;
        }
        return MonotonicNanos() - _query_begin_time > _big_query_profile_threshold_ns;
    }
    void set_big_query_profile_threshold(int64_t big_query_profile_threshold,
                                         TTimeUnit::type big_query_profile_threshold_unit) {
        int64_t factor = 1;
        switch (big_query_profile_threshold_unit) {
        case TTimeUnit::NANOSECOND:
            factor = 1;
            break;
        case TTimeUnit::MICROSECOND:
            factor = 1'000L;
            break;
        case TTimeUnit::MILLISECOND:
            factor = 1'000'000L;
            break;
        case TTimeUnit::SECOND:
            factor = 1'000'000'000L;
            break;
        case TTimeUnit::MINUTE:
            factor = 60 * 1'000'000'000L;
            break;
        default:
            DCHECK(false);
        }
        _big_query_profile_threshold_ns = factor * big_query_profile_threshold;
    }
    int64_t get_big_query_profile_threshold_ns() const { return _big_query_profile_threshold_ns; }
    void set_runtime_profile_report_interval(int64_t runtime_profile_report_interval_s) {
        _runtime_profile_report_interval_ns = 1'000'000'000L * runtime_profile_report_interval_s;
    }
    int64_t get_runtime_profile_report_interval_ns() { return _runtime_profile_report_interval_ns; }
    void set_profile_level(const TPipelineProfileLevel::type& profile_level) { _profile_level = profile_level; }
    const TPipelineProfileLevel::type& profile_level() { return _profile_level; }

    FragmentContextManager* fragment_mgr();

    void cancel(const Status& status, bool cancelled_by_fe);
    void set_cancelled_by_fe() { _cancelled_by_fe = true; }

    void set_is_runtime_filter_coordinator(bool flag) { _is_runtime_filter_coordinator = flag; }

    ObjectPool* object_pool() { return &_object_pool; }
    void set_desc_tbl(DescriptorTbl* desc_tbl) {
        DCHECK(_desc_tbl == nullptr);
        _desc_tbl = desc_tbl;
    }

    DescriptorTbl* desc_tbl() { return _desc_tbl; }

    size_t total_fragments() { return _total_fragments; }
    /// Initialize the mem_tracker of this query.
    /// Positive `big_query_mem_limit` and non-null `wg` indicate
    /// that there is a big query memory limit of this resource group.
    void init_mem_tracker(int64_t query_mem_limit, MemTracker* parent, int64_t big_query_mem_limit = -1,
                          std::optional<double> spill_mem_limit = std::nullopt, workgroup::WorkGroup* wg = nullptr,
                          RuntimeState* state = nullptr, int connector_scan_node_number = 1);
    std::shared_ptr<MemTracker> mem_tracker() { return _mem_tracker; }
    MemTracker* connector_scan_mem_tracker() { return _connector_scan_mem_tracker.get(); }

    Status init_spill_manager(const TQueryOptions& query_options);
    Status init_query_once(workgroup::WorkGroup* wg, bool enable_group_level_query_queue);
    /// Release the workgroup token only once to avoid double-free.
    /// This method should only be invoked while the QueryContext is still valid,
    /// to avoid double-free between the destruction and this method.
    void release_workgroup_token_once();

    // Some statistic about the query, including cpu, scan_rows, scan_bytes
    int64_t mem_cost_bytes() const { return _mem_tracker->peak_consumption(); }
    int64_t current_mem_usage_bytes() const { return _mem_tracker->consumption(); }
    void incr_cpu_cost(int64_t cost) {
        _total_cpu_cost_ns += cost;
        _delta_cpu_cost_ns += cost;
    }
    void incr_cur_scan_rows_num(int64_t rows_num) {
        _total_scan_rows_num += rows_num;
        _delta_scan_rows_num += rows_num;
    }
    void incr_cur_scan_bytes(int64_t scan_bytes) {
        _total_scan_bytes += scan_bytes;
        _delta_scan_bytes += scan_bytes;
    }

    void incr_transmitted_bytes(int64_t transmitted_bytes) { _total_transmitted_bytes += transmitted_bytes; }

    void init_node_exec_stats(const std::vector<int32_t>& exec_stats_node_ids);
    bool need_record_exec_stats(int32_t plan_node_id) {
        auto it = _node_exec_stats.find(plan_node_id);
        return it != _node_exec_stats.end();
    }

    void update_scan_stats(int64_t table_id, int64_t scan_rows_num, int64_t scan_bytes);
    void update_push_rows_stats(int32_t plan_node_id, int64_t push_rows) {
        auto it = _node_exec_stats.find(plan_node_id);
        if (it != _node_exec_stats.end()) {
            it->second->push_rows += push_rows;
        }
    }

    void update_pull_rows_stats(int32_t plan_node_id, int64_t pull_rows) {
        auto it = _node_exec_stats.find(plan_node_id);
        if (it != _node_exec_stats.end()) {
            it->second->pull_rows += pull_rows;
        }
    }

    void update_pred_filter_stats(int32_t plan_node_id, int64_t pred_filter_rows) {
        auto it = _node_exec_stats.find(plan_node_id);
        if (it != _node_exec_stats.end()) {
            it->second->pred_filter_rows += pred_filter_rows;
        }
    }

    void update_index_filter_stats(int32_t plan_node_id, int64_t index_filter_rows) {
        auto it = _node_exec_stats.find(plan_node_id);
        if (it != _node_exec_stats.end()) {
            it->second->index_filter_rows += index_filter_rows;
        }
    }

    void update_rf_filter_stats(int32_t plan_node_id, int64_t rf_filter_rows) {
        auto it = _node_exec_stats.find(plan_node_id);
        if (it != _node_exec_stats.end()) {
            it->second->rf_filter_rows += rf_filter_rows;
        }
    }

    void force_set_pull_rows_stats(int32_t plan_node_id, int64_t pull_rows) {
        auto it = _node_exec_stats.find(plan_node_id);
        if (it != _node_exec_stats.end()) {
            it->second->pull_rows.exchange(pull_rows);
        }
    }

    int64_t cpu_cost() const { return _total_cpu_cost_ns; }
    int64_t cur_scan_rows_num() const { return _total_scan_rows_num; }
    int64_t get_scan_bytes() const { return _total_scan_bytes; }
    std::atomic_int64_t* mutable_total_spill_bytes() { return &_total_spill_bytes; }
    int64_t get_spill_bytes() { return _total_spill_bytes; }
    int64_t get_transmitted_bytes() { return _total_transmitted_bytes; }

    // Query start time, used to check how long the query has been running
    // To ensure that the minimum run time of the query will not be killed by the big query checking mechanism
    int64_t query_begin_time() const { return _query_begin_time; }
    void init_query_begin_time() { _query_begin_time = MonotonicNanos(); }

    void set_scan_limit(int64_t scan_limit) { _scan_limit = scan_limit; }
    int64_t get_scan_limit() const { return _scan_limit; }
    void set_query_trace(std::shared_ptr<starrocks::debug::QueryTrace> query_trace);

    starrocks::debug::QueryTrace* query_trace() { return _query_trace.get(); }

    std::shared_ptr<starrocks::debug::QueryTrace> shared_query_trace() { return _query_trace; }

    // Delta statistic since last retrieve
    std::shared_ptr<QueryStatistics> intermediate_query_statistic(int64_t delta_transmitted_bytes);
    // Merged statistic from all executor nodes
    std::shared_ptr<QueryStatistics> final_query_statistic();
    std::shared_ptr<QueryStatisticsRecvr> maintained_query_recv();
    bool is_final_sink() const { return _is_final_sink; }
    void set_final_sink() { _is_final_sink = true; }

    QueryContextPtr get_shared_ptr() { return shared_from_this(); }

    // STREAM MV
    StreamEpochManager* stream_epoch_manager() const { return _stream_epoch_manager.get(); }

    spill::QuerySpillManager* spill_manager() { return _spill_manager.get(); }

    void mark_prepared() { _is_prepared = true; }
    bool is_prepared() { return _is_prepared; }

    int64_t get_static_query_mem_limit() const { return _static_query_mem_limit; }
    ConnectorScanOperatorMemShareArbitrator* connector_scan_operator_mem_share_arbitrator() const {
        return _connector_scan_operator_mem_share_arbitrator;
    }

public:
    static constexpr int DEFAULT_EXPIRE_SECONDS = 300;

private:
    ExecEnv* _exec_env = nullptr;
    TUniqueId _query_id;
    MonotonicStopWatch _lifetime_sw;
    std::unique_ptr<FragmentContextManager> _fragment_mgr;
    size_t _total_fragments;
    std::atomic<size_t> _num_fragments;
    std::atomic<size_t> _num_active_fragments;
    int64_t _delivery_deadline = 0;
    int64_t _query_deadline = 0;
    seconds _delivery_expire_seconds = seconds(DEFAULT_EXPIRE_SECONDS);
    seconds _query_expire_seconds = seconds(DEFAULT_EXPIRE_SECONDS);
    bool _is_runtime_filter_coordinator = false;
    std::once_flag _init_mem_tracker_once;
    bool _enable_pipeline_level_shuffle = true;
    std::shared_ptr<RuntimeProfile> _profile;
    bool _enable_profile = false;
    int64_t _big_query_profile_threshold_ns = 0;
    int64_t _runtime_profile_report_interval_ns = std::numeric_limits<int64_t>::max();
    TPipelineProfileLevel::type _profile_level;
    ObjectPool _object_pool;
    DescriptorTbl* _desc_tbl = nullptr;
    std::once_flag _query_trace_init_flag;
    std::shared_ptr<starrocks::debug::QueryTrace> _query_trace;
    std::atomic_bool _is_prepared = false;
    std::atomic_bool _is_cancelled = false;
    std::atomic_bool _cancelled_by_fe = false;
    std::atomic<Status*> _cancelled_status = nullptr;
    Status _s_status;

    std::once_flag _init_query_once;
    int64_t _query_begin_time = 0;
    std::once_flag _init_spill_manager_once;
    std::atomic<int64_t> _total_cpu_cost_ns = 0;
    std::atomic<int64_t> _total_scan_rows_num = 0;
    std::atomic<int64_t> _total_scan_bytes = 0;
    std::atomic<int64_t> _total_spill_bytes = 0;
    std::atomic<int64_t> _total_transmitted_bytes = 0;
    std::atomic<int64_t> _delta_cpu_cost_ns = 0;
    std::atomic<int64_t> _delta_scan_rows_num = 0;
    std::atomic<int64_t> _delta_scan_bytes = 0;

    struct ScanStats {
        std::atomic<int64_t> total_scan_rows_num = 0;
        std::atomic<int64_t> total_scan_bytes = 0;
        std::atomic<int64_t> delta_scan_rows_num = 0;
        std::atomic<int64_t> delta_scan_bytes = 0;
    };

    std::once_flag _node_exec_stats_init_flag;
    struct NodeExecStats {
        std::atomic_int64_t push_rows;
        std::atomic_int64_t pull_rows;
        std::atomic_int64_t pred_filter_rows;
        std::atomic_int64_t index_filter_rows;
        std::atomic_int64_t rf_filter_rows;
    };

    // @TODO(silverbullet233):
    // our phmap's version is too old and it doesn't provide a thread-safe iteration interface,
    // we use spinlock + flat_hash_map here, after upgrading, we can change it to parallel_flat_hash_map
    SpinLock _scan_stats_lock;
    // table level scan stats
    phmap::flat_hash_map<int64_t, std::shared_ptr<ScanStats>, StdHash<int64_t>> _scan_stats;

    std::unordered_map<int32_t, std::shared_ptr<NodeExecStats>> _node_exec_stats;

    bool _is_final_sink = false;
    std::shared_ptr<QueryStatisticsRecvr> _sub_plan_query_statistics_recvr; // For receive

    int64_t _scan_limit = 0;
    // _wg_mem_tracker is used to grab mem_tracker in workgroup to prevent it from
    // being released prematurely in FragmentContext::cancel, otherwise accessing
    // workgroup's mem_tracker in QueryContext's dtor shall cause segmentation fault.
    std::shared_ptr<MemTracker> _wg_mem_tracker = nullptr;
    workgroup::RunningQueryTokenPtr _wg_running_query_token_ptr;
    std::atomic<workgroup::RunningQueryToken*> _wg_running_query_token_atomic_ptr = nullptr;
    std::shared_ptr<MemTracker> _mem_tracker;
    std::shared_ptr<MemTracker> _connector_scan_mem_tracker;

    // STREAM MV
    std::shared_ptr<StreamEpochManager> _stream_epoch_manager;

    std::unique_ptr<spill::QuerySpillManager> _spill_manager;

    int64_t _static_query_mem_limit = 0;
    ConnectorScanOperatorMemShareArbitrator* _connector_scan_operator_mem_share_arbitrator = nullptr;
};

// TODO: use brpc::TimerThread refactor QueryContext
class QueryContextManager {
public:
    QueryContextManager(size_t log2_num_slots);
    ~QueryContextManager();
    Status init();
    StatusOr<QueryContext*> get_or_register(const TUniqueId& query_id, bool return_error_if_not_exist = false);
    QueryContextPtr get(const TUniqueId& query_id, bool need_prepared = false);
    size_t size();
    bool remove(const TUniqueId& query_id);
    // used for graceful exit
    void clear();

    void report_fragments(const std::vector<starrocks::PipeLineReportTaskKey>& non_pipeline_need_report_fragment_ids);

    void report_fragments_with_same_host(
            const std::vector<std::shared_ptr<FragmentContext>>& need_report_fragment_context,
            std::vector<bool>& reported, const TNetworkAddress& last_coord_addr,
            std::vector<TReportExecStatusParams>& report_exec_status_params_vector,
            std::vector<int32_t>& cur_batch_report_indexes);

    void collect_query_statistics(const PCollectQueryStatisticsRequest* request,
                                  PCollectQueryStatisticsResult* response);
    void for_each_active_ctx(const std::function<void(QueryContextPtr)>& func);

private:
    static void _clean_func(QueryContextManager* manager);
    void _clean_query_contexts();
    void _stop_clean_func() { _stop.store(true); }
    bool _is_stopped() { return _stop; }
    size_t _slot_idx(const TUniqueId& query_id);
    void _clean_slot_unlocked(size_t i, std::vector<QueryContextPtr>& del);

private:
    const size_t _num_slots;
    const size_t _slot_mask;
    std::vector<std::shared_mutex> _mutexes;
    std::vector<std::unordered_map<TUniqueId, QueryContextPtr>> _context_maps;
    std::vector<std::unordered_map<TUniqueId, QueryContextPtr>> _second_chance_maps;

    std::atomic<bool> _stop{false};
    std::shared_ptr<std::thread> _clean_thread;

    inline static const char* _metric_name = "pip_query_ctx_cnt";
    std::unique_ptr<UIntGauge> _query_ctx_cnt;
};

} // namespace pipeline
} // namespace starrocks
