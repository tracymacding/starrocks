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

#include <fslib/configuration.h>
#include <fslib/file_system.h>
#include <starlet.h>

#include "common/statusor.h"
#include "fs/fs.h"
#include "fs/fs_starlet.h"
#include "util/lru_cache.h"

namespace starrocks {

class StarletFsMgr {
public:
    StarletFsMgr();
    ~StarletFsMgr() = default;

    // is_starlet_uri() performs less strict verification than parse_starlet_uri(), which means
    // if is_starlet_uri() returns false parse_starlet_uri() must fail and if is_starlet_uri()
    // returns true parse_starlet_uri() may also fail.
    static bool is_starlet_uri(std::string_view uri);

    static std::string build_starlet_uri(uint64_t shard_id, std::string_view path);

    // Expected format of uri: staros://ShardID/path/to/file
    // The first element of pair is path, the second element of pair is shard id.
    //      staros://shardid/over/there
    //      \__/    \_____/ \_______/
    //       |         |        |
    //     scheme   shard_id   path
    //
    // If parse_starlet_uri() succeeded, is_starlet_uri() must be true.
    static StatusOr<std::pair<std::string, int64_t>> parse_starlet_uri(std::string_view uri);

    StatusOr<std::unique_ptr<FileSystem>> get_starlet_fs(std::string_view path);

private:
    static Status to_status(absl::Status absl_st);

    Status build_starlet_fs_conf(const staros::starlet::ShardInfo& shard_info, staros::starlet::fslib::Configuration& conf);

    StatusOr<std::unique_ptr<StarletFileSystem>> new_fs_from_shard_info(const staros::starlet::ShardInfo& info);

    StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>> new_shared_filesystem(staros::starlet::fslib::Configuration& conf);

private:
    std::unique_ptr<Cache> _fs_cache;
};

} // namespace starrocks

#endif // USE_STAROS
