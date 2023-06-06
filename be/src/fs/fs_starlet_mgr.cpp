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

#include <fmt/core.h>
#include <fslib/configuration.h>
#include <fslib/file_system.h>
#include <fslib/fslib_all_initializer.h>
#include <fslib/stat.h>
#include <starlet.h>
#include <sys/stat.h>

#include "common/config.h"
#include "fs/fs_starlet.h"
#include "fs/fs_starlet_mgr.h"
#include "gutil/strings/util.h"
#include "service/staros_worker.h"
#include "util/debug_util.h"
#include "util/lru_cache.h"
#include "util/sha.h"
#include "util/string_parser.hpp"

namespace starrocks {

StarletFsMgr::StarletFsMgr() : _fs_cache(new_lru_cache(1024)) {
    staros::starlet::fslib::register_builtin_filesystems();
}

bool StarletFsMgr::is_starlet_uri(std::string_view path) {
    return HasPrefixString(path, "staros://");
}

std::string StarletFsMgr::build_starlet_uri(uint64_t shard_id, std::string_view path) {
    while (!path.empty() && path.front() == '/') {
        path.remove_prefix(1);
    }
    return path.empty() ? fmt::format("staros://{}", shard_id) : fmt::format("staros://{}/{}", shard_id, path);
}

// Expected format of uri: staros://ShardID/path/to/file
StatusOr<std::pair<std::string, int64_t>> StarletFsMgr::parse_starlet_uri(std::string_view uri) {
    std::string_view path = uri;
    if (!is_starlet_uri(path)) {
        return Status::InvalidArgument(fmt::format("Invalid starlet URI: {}", uri));
    }
    path.remove_prefix(sizeof("staros://") - 1);
    auto end_shard_id = path.find('/');
    if (end_shard_id == std::string::npos) {
        end_shard_id = path.size();
    }

    StringParser::ParseResult result;
    auto shard_id = StringParser::string_to_int<int64_t>(path.data(), end_shard_id, &result);
    if (result != StringParser::PARSE_SUCCESS) {
        return Status::InvalidArgument(fmt::format("Invalid starlet URI: {}", uri));
    }
    path.remove_prefix(std::min<size_t>(path.size(), end_shard_id + 1));

    return std::make_pair(std::string(path), shard_id);
};

StatusOr<std::unique_ptr<FileSystem>> StarletFsMgr::get_starlet_fs(std::string_view path) {
    auto pair_or_st = parse_starlet_uri(path);
    if (!pair_or_st.ok()) {
        return pair_or_st.status();
    }

    auto shard_info_or_st = g_worker->get_shard_info((*pair_or_st).second);
    if (!shard_info_or_st.ok()) {
        return Status::InternalError("Failed to get shard info");
    }
    return new_fs_from_shard_info(*shard_info_or_st);
}

Status StarletFsMgr::build_starlet_fs_conf(const staros::starlet::ShardInfo& shard_info, staros::starlet::fslib::Configuration& conf) {
    std::string scheme = "file://";
    switch (shard_info.path_info.fs_info().fs_type()) {
    case staros::FileStoreType::S3:
        scheme = "s3://";
        {
            auto& s3_info = shard_info.path_info.fs_info().s3_fs_info();
            if (!shard_info.path_info.full_path().empty()) {
                conf[staros::starlet::fslib::kSysRoot] = shard_info.path_info.full_path();
            }
            if (!s3_info.bucket().empty()) {
                conf[staros::starlet::fslib::kS3Bucket] = s3_info.bucket();
            }
            if (!s3_info.region().empty()) {
                conf[staros::starlet::fslib::kS3Region] = s3_info.region();
            }
            if (!s3_info.endpoint().empty()) {
                conf[staros::starlet::fslib::kS3OverrideEndpoint] = s3_info.endpoint();
            }
            if (s3_info.has_credential()) {
                auto credential = s3_info.credential();
                if (credential.has_default_credential()) {
                    conf[staros::starlet::fslib::kS3CredentialType] = "default";
                } else if (credential.has_simple_credential()) {
                    conf[staros::starlet::fslib::kS3CredentialType] = "simple";
                    auto simple_credential = credential.simple_credential();
                    conf[staros::starlet::fslib::kS3CredentialSimpleAccessKeyId] = simple_credential.access_key();
                    conf[staros::starlet::fslib::kS3CredentialSimpleAccessKeySecret] = simple_credential.access_key_secret();
                } else if (credential.has_profile_credential()) {
                    conf[staros::starlet::fslib::kS3CredentialType] = "instance_profile";
                } else if (credential.has_assume_role_credential()) {
                    conf[staros::starlet::fslib::kS3CredentialType] = "assume_role";
                    auto role_credential = credential.assume_role_credential();
                    conf[staros::starlet::fslib::kS3CredentialAssumeRoleArn] = role_credential.iam_role_arn();
                    conf[staros::starlet::fslib::kS3CredentialAssumeRoleExternalId] = role_credential.external_id();
                } else {
                    conf[staros::starlet::fslib::kS3CredentialType] = "default";
                }
            }
        }
        break;
    case staros::FileStoreType::HDFS:
        scheme = "hdfs://";
        conf[staros::starlet::fslib::kSysRoot] = shard_info.path_info.full_path();
        break;
    case staros::FileStoreType::AZBLOB:
        scheme = "azblob://";
        {
            conf[staros::starlet::fslib::kSysRoot] = shard_info.path_info.full_path();
            conf[staros::starlet::fslib::kAzBlobEndpoint] = shard_info.path_info.fs_info().azblob_fs_info().endpoint();
            auto& credential = shard_info.path_info.fs_info().azblob_fs_info().credential();
            conf[staros::starlet::fslib::kAzBlobSharedKey] = credential.shared_key();
            conf[staros::starlet::fslib::kAzBlobSASToken] = credential.sas_token();
            conf[staros::starlet::fslib::kAzBlobTenantId] = credential.tenant_id();
            conf[staros::starlet::fslib::kAzBlobClientId] = credential.client_id();
            conf[staros::starlet::fslib::kAzBlobClientSecret] = credential.client_secret();
            conf[staros::starlet::fslib::kAzBlobClientCertificatePath] = credential.client_certificate_path();
            conf[staros::starlet::fslib::kAzBlobAuthorityHost] = credential.authority_host();
        }
        break;
    default:
        return Status::InvalidArgument("Unknown shard storage scheme!");
    }

    std::string cache_dir = config::starlet_cache_dir;
    auto cache_info = shard_info.cache_info;
    bool cache_enabled = cache_info.enable_cache() && !cache_dir.empty();
    if (cache_enabled) {
        // FIXME: currently the cache root dir is set from be.conf, could be changed in future
        const static std::string conf_prefix("cachefs.");
        scheme = "cachefs://";
        // rebuild configuration for cachefs
        staros::starlet::fslib::Configuration tmp;
        tmp.swap(conf);
        for (auto& iter : tmp) {
            conf[conf_prefix + iter.first] = iter.second;
        }
        conf[staros::starlet::fslib::kSysRoot] = "/";
        // original fs sys.root as cachefs persistent uri
        conf[staros::starlet::fslib::kCacheFsPersistUri] = tmp[staros::starlet::fslib::kSysRoot];
        // use persistent uri as identifier to maximize sharing of cache data
        conf[staros::starlet::fslib::kCacheFsIdentifier] = tmp[staros::starlet::fslib::kSysRoot];
        conf[staros::starlet::fslib::kCacheFsTtlSecs] = absl::StrFormat("%ld", cache_info.ttl_seconds());
        if (cache_info.async_write_back()) {
            conf[staros::starlet::fslib::kCacheFsAsyncWriteBack] = "true";
        }
        // set environ variable to cachefs directory
        setenv(staros::starlet::fslib::kFslibCacheDir.c_str(), cache_dir.c_str(), 0 /*overwrite*/);
    }
    conf["scheme"] = scheme;
    return Status::OK();

}

StatusOr<std::unique_ptr<StarletFileSystem>> StarletFsMgr::new_fs_from_shard_info(const staros::starlet::ShardInfo& info) {
    staros::starlet::fslib::Configuration conf;
    auto st = build_starlet_fs_conf(info, conf);
    if (!st.ok()) {
        return st;
    }
    auto staros_fs = new_shared_filesystem(conf);
    if (!staros_fs.ok()) {
        return staros_fs.status();
    }

    auto starlet_prefix = fmt::format("staros://{}/", info.id);
    auto starlet_fs_real_root = info.path_info.full_path();
    return std::make_unique<StarletFileSystem>(*staros_fs, starlet_prefix, starlet_fs_real_root);
}

using CacheValue = std::weak_ptr<staros::starlet::fslib::FileSystem>;

static void cache_value_deleter(const CacheKey& /*key*/, void* value) { delete static_cast<CacheValue*>(value); }

Status StarletFsMgr::to_status(absl::Status absl_st) {
    switch (absl_st.code()) {
    case absl::StatusCode::kOk:
        return Status::OK();
    case absl::StatusCode::kAlreadyExists:
        return Status::AlreadyExist(fmt::format("starlet err {}", absl_st.message()));
    case absl::StatusCode::kOutOfRange:
        return Status::InvalidArgument(fmt::format("starlet err {}", absl_st.message()));
    case absl::StatusCode::kInvalidArgument:
        return Status::InvalidArgument(fmt::format("starlet err {}", absl_st.message()));
    case absl::StatusCode::kNotFound:
        return Status::NotFound(fmt::format("starlet err {}", absl_st.message()));
    default:
        return Status::InternalError(fmt::format("starlet err {}", absl_st.message()));
    }
}

StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>> StarletFsMgr::new_shared_filesystem(staros::starlet::fslib::Configuration& conf) {
    // Take the SHA-256 hash value as the cache key
    SHA256Digest sha256;
    for (const auto& [k, v] : conf) {
        sha256.update(k.data(), k.size());
        sha256.update(v.data(), v.size());
    }
    sha256.digest();
    CacheKey key(sha256.hex());

    // Lookup LRU cache
    std::shared_ptr<staros::starlet::fslib::FileSystem> fs;
    auto handle = _fs_cache->lookup(key);
    if (handle != nullptr) {
        auto value = static_cast<CacheValue*>(_fs_cache->value(handle));
        fs = value->lock();
        _fs_cache->release(handle);
        if (fs != nullptr) {
            return std::move(fs);
        }
    }

    VLOG(9) << "Create a new starlet filesystem";

    // Create a new instance of FileSystem
    auto fs_or = staros::starlet::fslib::FileSystemFactory::new_filesystem(conf["scheme"], conf);
    if (!fs_or.ok()) {
        return to_status(fs_or.status());
    }
    // turn unique_ptr to shared_ptr
    fs = std::move(fs_or).value();

    // Put the FileSysatem into LRU cache
    auto value = new CacheValue(fs);
    handle = _fs_cache->insert(key, value, 1, cache_value_deleter);
    if (handle == nullptr) {
        delete value;
    } else {
        _fs_cache->release(handle);
    }

    return std::move(fs);
}

} // namespace starrocks

#endif // USE_STAROS
