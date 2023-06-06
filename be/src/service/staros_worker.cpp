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

#ifdef USE_STAROS
#include "service/staros_worker.h"

#include <fslib/fslib_all_initializer.h>
#include <starlet.h>

#include "common/config.h"
#include "gflags/gflags.h"
#include "util/debug_util.h"

// cachemgr thread pool size
DECLARE_int32(cachemgr_threadpool_size);
// cache backend check interval (in seconds), for async write sync check and ttl clean, e.t.c.
DECLARE_int32(cachemgr_check_interval);
// cache backend cache evictor interval (in seconds)
DECLARE_int32(cachemgr_evict_interval);
// cache will start evict cache files if free space belows this value(percentage)
DECLARE_double(cachemgr_evict_low_water);
// cache will stop evict cache files if free space is above this value(percentage)
DECLARE_double(cachemgr_evict_high_water);
// type:Integer. CacheManager cache directory allocation policy. (0:default, 1:random, 2:round-robin)
DECLARE_int32(cachemgr_dir_allocate_policy);
// buffer size in starlet fs buffer stream, size <= 0 means not use buffer stream.
DECLARE_int32(fs_stream_buffer_size_bytes);

namespace starrocks {

std::shared_ptr<StarOSWorker> g_worker;
std::unique_ptr<staros::starlet::Starlet> g_starlet;

namespace fslib = staros::starlet::fslib;

StarOSWorker::StarOSWorker() : _mtx(), _shards() {}

StarOSWorker::~StarOSWorker() = default;

absl::Status StarOSWorker::add_shard(const ShardInfo& shard) {
    std::unique_lock l(_mtx);
    _shards.try_emplace(shard.id, shard);
    return absl::OkStatus();
}

absl::Status StarOSWorker::remove_shard(const ShardId id) {
    std::unique_lock l(_mtx);
    _shards.erase(id);
    return absl::OkStatus();
}

absl::StatusOr<staros::starlet::ShardInfo> StarOSWorker::get_shard_info(ShardId id) const {
    std::shared_lock l(_mtx);
    auto it = _shards.find(id);
    if (it == _shards.end()) {
        l.unlock();
        auto info_or = g_starlet->get_shard_info(id);
        if (!info_or.ok()) {
            return info_or.status();
        }
        // lazy mode, do not put it into cache
        return *info_or;
    }
    return it->second;
}

std::vector<staros::starlet::ShardInfo> StarOSWorker::shards() const {
    std::vector<staros::starlet::ShardInfo> vec;
    vec.reserve(_shards.size());

    std::shared_lock l(_mtx);
    for (const auto& shard : _shards) {
        vec.emplace_back(shard.second);
    }
    return vec;
}

absl::StatusOr<staros::starlet::WorkerInfo> StarOSWorker::worker_info() const {
    staros::starlet::WorkerInfo worker_info;

    std::shared_lock l(_mtx);
    worker_info.worker_id = worker_id();
    worker_info.service_id = service_id();
    worker_info.properties["port"] = std::to_string(config::starlet_port);
    worker_info.properties["be_port"] = std::to_string(config::be_port);
    worker_info.properties["be_http_port"] = std::to_string(config::be_http_port);
    worker_info.properties["be_brpc_port"] = std::to_string(config::brpc_port);
    worker_info.properties["be_heartbeat_port"] = std::to_string(config::heartbeat_service_port);
    worker_info.properties["be_version"] = get_short_version();
    for (auto& iter : _shards) {
        worker_info.shards.insert(iter.first);
    }
    return worker_info;
}

absl::Status StarOSWorker::update_worker_info(const staros::starlet::WorkerInfo& new_worker_info) {
    return absl::OkStatus();
}

void init_staros_worker() {
    if (g_starlet.get() != nullptr) {
        return;
    }
    // skip staros reinit aws sdk
    staros::starlet::fslib::skip_aws_init_api = true;

    FLAGS_cachemgr_threadpool_size = config::starlet_cache_thread_num;
    FLAGS_cachemgr_check_interval = config::starlet_cache_check_interval;
    FLAGS_cachemgr_evict_interval = config::starlet_cache_evict_interval;
    FLAGS_cachemgr_evict_low_water = config::starlet_cache_evict_low_water;
    FLAGS_cachemgr_evict_high_water = config::starlet_cache_evict_high_water;
    FLAGS_cachemgr_dir_allocate_policy = config::starlet_cache_dir_allocate_policy;
    FLAGS_fs_stream_buffer_size_bytes = config::starlet_fs_stream_buffer_size_bytes;

    staros::starlet::StarletConfig starlet_config;
    starlet_config.rpc_port = config::starlet_port;
    g_worker = std::make_shared<StarOSWorker>();
    g_starlet = std::make_unique<staros::starlet::Starlet>(g_worker);
    g_starlet->init(starlet_config);
    g_starlet->start();
}

void shutdown_staros_worker() {
    g_starlet->stop();
    g_starlet.reset();
    g_worker = nullptr;
}

} // namespace starrocks
#endif // USE_STAROS
