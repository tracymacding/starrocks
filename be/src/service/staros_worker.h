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

#ifdef USE_STAROS

#include <starlet.h>

#include <memory>
#include <shared_mutex>
#include <unordered_map>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "fslib/file_system.h"

namespace starrocks {

class StarOSWorker : public staros::starlet::Worker {
public:
    using ShardId = staros::starlet::ShardId;
    using ShardInfo = staros::starlet::ShardInfo;
    using WorkerInfo = staros::starlet::WorkerInfo;

    StarOSWorker();

    ~StarOSWorker() override;

    absl::Status add_shard(const ShardInfo& shard) override;

    absl::Status remove_shard(const ShardId shard) override;

    absl::StatusOr<WorkerInfo> worker_info() const override;

    absl::Status update_worker_info(const WorkerInfo& info) override;

    absl::StatusOr<ShardInfo> get_shard_info(ShardId id) const override;

    std::vector<ShardInfo> shards() const override;

    mutable std::shared_mutex _mtx;
    std::unordered_map<ShardId, ShardInfo> _shards;
};

extern std::shared_ptr<StarOSWorker> g_worker;
void init_staros_worker();
void shutdown_staros_worker();

} // namespace starrocks
#endif // USE_STAROS
