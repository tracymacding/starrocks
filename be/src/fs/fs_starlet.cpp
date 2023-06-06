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

#include "absl/strings/strip.h"
#include "fs/fs_starlet.h"
#include "storage/olap_common.h"

namespace starrocks {

bvar::Adder<int64_t> g_starlet_io_read;       // number of bytes read through Starlet
bvar::Adder<int64_t> g_starlet_io_write;      // number of bytes wrote through Starlet
bvar::Adder<int64_t> g_starlet_io_num_reads;  // number of Starlet's read API invocations
bvar::Adder<int64_t> g_starlet_io_num_writes; // number of Starlet's write API invocations

bvar::PerSecond<bvar::Adder<int64_t>> g_starlet_read_second("starlet_io_read_bytes_second", &g_starlet_io_read);
bvar::PerSecond<bvar::Adder<int64_t>> g_starlet_write_second("starlet_io_write_bytes_second", &g_starlet_io_write);
bvar::PerSecond<bvar::Adder<int64_t>> g_starlet_num_reads_second("starlet_io_read_second", &g_starlet_io_num_reads);
bvar::PerSecond<bvar::Adder<int64_t>> g_starlet_num_writes_second("starlet_io_write_second", &g_starlet_io_num_writes);

using Anchor = staros::starlet::fslib::Stream::Anchor;

Status StarletInputStream::seek(int64_t position) {
    auto stream_st = _file_ptr->stream();
    if (!stream_st.ok()) {
        return StarletFileSystem::from_absl_status(stream_st.status());
    }
    return StarletFileSystem::from_absl_status((*stream_st)->seek(position, Anchor::BEGIN).status());
}

StatusOr<int64_t> StarletInputStream::position() {
    auto stream_st = _file_ptr->stream();
    if (!stream_st.ok()) {
        return StarletFileSystem::from_absl_status(stream_st.status());
    }
    if ((*stream_st)->support_tell()) {
        auto tell_st = (*stream_st)->tell();
        if (tell_st.ok()) {
            return *tell_st;
        } else {
            return StarletFileSystem::from_absl_status(tell_st.status());
        }
    } else {
        return Status::NotSupported("StarletInputStream::position");
    }
}

StatusOr<int64_t> StarletInputStream::get_size() {
    //note: starlet s3 filesystem do not return not found err when file not exists;
    auto st = _file_ptr->size();
    if (st.ok()) {
        return *st;
    } else {
        LOG(WARNING) << "get file: " << _file_ptr->name() << " size error: " << st.status();
        return StarletFileSystem::from_absl_status(st.status());
    }
}

StatusOr<int64_t> StarletInputStream::read(void* data, int64_t count) {
    auto stream_st = _file_ptr->stream();
    if (!stream_st.ok()) {
        return StarletFileSystem::from_absl_status(stream_st.status());
    }
    auto res = (*stream_st)->read(data, count);
    if (res.ok()) {
        g_starlet_io_num_reads << 1;
        g_starlet_io_read << *res;
        return *res;
    } else {
        return StarletFileSystem::from_absl_status(res.status());
    }
}

StatusOr<std::unique_ptr<io::NumericStatistics>> StarletInputStream::get_numeric_statistics() {
    auto stream_st = _file_ptr->stream();
    if (!stream_st.ok()) {
        return StarletFileSystem::from_absl_status(stream_st.status());
    }

    const auto& read_stats = (*stream_st)->get_read_stats();
    auto stats = std::make_unique<io::NumericStatistics>();
    stats->reserve(6);
    stats->append(kBytesReadLocalDisk, read_stats.bytes_read_local_disk);
    stats->append(kBytesReadRemote, read_stats.bytes_read_remote);
    stats->append(kIOCountLocalDisk, read_stats.io_count_local_disk);
    stats->append(kIOCountRemote, read_stats.io_count_remote);
    stats->append(kIONsLocalDisk, read_stats.io_ns_local_disk);
    stats->append(kIONsRemote, read_stats.io_ns_remote);
    return std::move(stats);
}

Status StarletOutputStream::write(const void* data, int64_t size) {
    auto stream_st = _file_ptr->stream();
    if (!stream_st.ok()) {
        return StarletFileSystem::from_absl_status(stream_st.status());
    }
    auto left = size;
    while (left > 0) {
        auto* p = static_cast<const char*>(data) + size - left;
        auto res = (*stream_st)->write(p, left);
        if (!res.ok()) {
            return StarletFileSystem::from_absl_status(res.status());
        }
        left -= *res;
    }
    g_starlet_io_num_writes << 1;
    g_starlet_io_write << size;
    return Status::OK();
}

bool StarletOutputStream::allows_aliasing() const { return false; }

Status StarletOutputStream::write_aliased(const void* data, int64_t size) {
    return Status::NotSupported("StarletOutputStream::write_aliased");
}

Status StarletOutputStream::close() {
    auto stream_st = _file_ptr->stream();
    if (!stream_st.ok()) {
        return StarletFileSystem::from_absl_status(stream_st.status());
    }
    return StarletFileSystem::from_absl_status((*stream_st)->close());
}

Status StarletFileSystem::canonicalize(const std::string& path, std::string* result) {
    if (absl::StartsWithIgnoreCase(path, _starlet_prefix)) {
        auto relative_path = absl::StripPrefix(path, _starlet_prefix);
        *result = join_path(_root_path, relative_path);
    } else if (absl::StartsWithIgnoreCase(path, _root_path)) {
        *result = path;
    } else {
        *result = join_path(_root_path, path);
    }
    return Status::OK();
}

std::string StarletFileSystem::normalize_path(std::string_view starlet_file_path) {
    std::string real_path = "";
    if (absl::StartsWithIgnoreCase(starlet_file_path, _starlet_prefix)) {
        auto relative_path = absl::StripPrefix(starlet_file_path, _starlet_prefix);
        real_path = join_path(_root_path, relative_path);
    } else if (absl::StartsWithIgnoreCase(starlet_file_path, _root_path)) {
        real_path = starlet_file_path;
    } else {
        real_path = join_path(_root_path, starlet_file_path);
    }
    return real_path;
}

std::string StarletFileSystem::relative_path(std::string_view starlet_file_path) {
    std::string relative_path = "";
    if (absl::StartsWithIgnoreCase(starlet_file_path, _starlet_prefix)) {
        relative_path = absl::StripPrefix(starlet_file_path, _starlet_prefix);
    } else if (absl::StartsWithIgnoreCase(starlet_file_path, _root_path)) {
        relative_path = absl::StripPrefix(starlet_file_path, _root_path);
    } else {
        relative_path = starlet_file_path;
    }
    if (absl::StartsWith(relative_path, "/")) {
        return std::string(absl::StripPrefix(relative_path, "/"));
    } else {
        return relative_path;
    }
}

StatusOr<std::unique_ptr<RandomAccessFile>> StarletFileSystem::new_random_access_file(const RandomAccessFileOptions& opts,
                                                                                      const std::string& path) {
    auto full_path = relative_path(path);
    auto file_st = _fs->open(full_path, ReadOptions{.skip_fill_local_cache = opts.skip_fill_local_cache});
    if (!file_st.ok()) {
        return StarletFileSystem::from_absl_status(file_st.status());
    }

    bool is_cache_hit = (*file_st)->is_cache_hit();
    auto istream = std::make_shared<StarletInputStream>(std::move(*file_st));
    return std::make_unique<RandomAccessFile>(std::move(istream), full_path, is_cache_hit);
}

StatusOr<std::unique_ptr<SequentialFile>> StarletFileSystem::new_sequential_file(const SequentialFileOptions& opts,
                                                                                 const std::string& path) {
    auto full_path = relative_path(path);

    auto file_st = _fs->open(full_path, ReadOptions{.skip_fill_local_cache = opts.skip_fill_local_cache});
    if (!file_st.ok()) {
        return StarletFileSystem::from_absl_status(file_st.status());
    }
    auto istream = std::make_shared<StarletInputStream>(std::move(*file_st));
    return std::make_unique<SequentialFile>(std::move(istream), full_path);
}

StatusOr<std::unique_ptr<WritableFile>> StarletFileSystem::new_writable_file(const std::string& path) {
    return new_writable_file(WritableFileOptions(), path);
}

StatusOr<std::unique_ptr<WritableFile>> StarletFileSystem::new_writable_file(const WritableFileOptions& opts,
                                                            const std::string& path) {
    staros::starlet::fslib::WriteOptions fslib_opts;
    fslib_opts.create_missing_parent = true;
    fslib_opts.skip_fill_local_cache = opts.skip_fill_local_cache;
  
    auto full_path = relative_path(path);

    auto file_st = _fs->create(full_path, fslib_opts);
    if (!file_st.ok()) {
        return StarletFileSystem::from_absl_status(file_st.status());
    }

    auto outputstream = std::make_unique<StarletOutputStream>(std::move(*file_st));
    return std::make_unique<starrocks::OutputStreamAdapter>(std::move(outputstream), full_path);
}

Status StarletFileSystem::delete_file(const std::string& path) {
    auto full_path = relative_path(path);
    auto st = _fs->delete_file(full_path);
    return StarletFileSystem::from_absl_status(st);
}

using EntryStat = staros::starlet::fslib::EntryStat;
Status StarletFileSystem::iterate_dir(const std::string& dir, const std::function<bool(std::string_view)>& cb) {
    auto full_dir = relative_path(dir);
    auto st = _fs->list_dir(full_dir, false, [&](EntryStat stat) { return cb(stat.name); });
    return StarletFileSystem::from_absl_status(st);
}

Status StarletFileSystem::iterate_dir2(const std::string& dir, const std::function<bool(DirEntry)>& cb) {
    auto full_path = relative_path(dir);
    auto st = _fs->list_dir(full_path, false, [&](EntryStat e) {
        DirEntry entry{.name = e.name,
                        .mtime = std::move(e.mtime),
                        .size = std::move(e.size),
                        .is_dir = std::move(e.is_dir)};
        return cb(entry);
    });
    return StarletFileSystem::from_absl_status(st);
}

Status StarletFileSystem::create_dir(const std::string& dir) {
    auto full_path = relative_path(dir);
    auto res = _fs->mkdir(full_path);
    return StarletFileSystem::from_absl_status(res);
}

Status StarletFileSystem::create_dir_if_missing(const std::string& dirname, bool* created) {
    auto full_path = relative_path(dirname);
    auto st = create_dir(full_path);
    if (created != nullptr) {
        *created = st.ok();
    }
    if (st.is_already_exist()) {
        st = Status::OK();
    }
    return st;
}

Status StarletFileSystem::create_dir_recursive(const std::string& dirname) {
    return create_dir_if_missing(dirname, nullptr);
}

Status StarletFileSystem::delete_dir(const std::string& dirname) {
    // TODO: leave this check to starlet.
    auto full_path = relative_path(dirname);
    bool dir_empty = true;
    auto cb = [&dir_empty](EntryStat stat) {
        dir_empty = false;
        // break the iteration
        return false;
    };
    auto st = _fs->list_dir(full_path, false, cb);
    if (!st.ok()) {
        return StarletFileSystem::from_absl_status(st);
    }
    if (!dir_empty) {
        return Status::InternalError(fmt::format("dir {} is not empty", full_path));
    }
    auto res = _fs->delete_dir(full_path, false);
    return StarletFileSystem::from_absl_status(res);
}

Status StarletFileSystem::delete_dir_recursive(const std::string& dirname) {
    auto full_path = relative_path(dirname);
    auto st = _fs->delete_dir(full_path, true);
    return StarletFileSystem::from_absl_status(st);
}

// determine given path is a directory or not
// returns
//  * true - exists and is a directory
//  * false - exists but is not a directory
//  * error status: not exist or other errors
StatusOr<bool> StarletFileSystem::is_directory(const std::string& path) {
    auto full_path = relative_path(path);
    auto fst = _fs->stat(full_path);
    if (!fst.ok()) {
        return StarletFileSystem::from_absl_status(fst.status());
    }
    return S_ISDIR(fst->mode);
}

Status StarletFileSystem::sync_dir(const std::string& dirname) {
    ASSIGN_OR_RETURN(const bool is_dir, is_directory(dirname));
    if (is_dir) return Status::OK();
    return Status::NotFound(fmt::format("{} not directory", dirname));
}

StatusOr<SpaceInfo> StarletFileSystem::space(const std::string& path) {
    const Status status = is_directory(path).status();
    if (!status.ok()) {
        return status;
    }
    return SpaceInfo{.capacity = std::numeric_limits<int64_t>::max(),
                        .free = std::numeric_limits<int64_t>::max(),
                        .available = std::numeric_limits<int64_t>::max()};
}

Status StarletFileSystem::path_exists(const std::string& path) {
    auto full_path = relative_path(path);
    auto exists_or = _fs->exists(full_path);
    if (!exists_or.ok()) {
        return StarletFileSystem::from_absl_status(exists_or.status());
    }
    return *exists_or ? Status::OK() : Status::NotFound(path);
}

Status StarletFileSystem::get_children(const std::string& dir, std::vector<std::string>* file) {
    return Status::NotSupported("StarletFileSystem::get_children");
}

StatusOr<uint64_t> StarletFileSystem::get_file_size(const std::string& path) {
    return Status::NotSupported("StarletFileSystem::get_file_size");
}

StatusOr<uint64_t> StarletFileSystem::get_file_modified_time(const std::string& path) {
    auto full_path = relative_path(path);
    auto fst = _fs->stat(full_path);
    if (!fst.ok()) {
        return StarletFileSystem::from_absl_status(fst.status());
    }
    return fst->mtime;
}

Status StarletFileSystem::rename_file(const std::string& src, const std::string& target) {
    return Status::NotSupported("StarletFileSystem::rename_file");
}

Status StarletFileSystem::link_file(const std::string& old_path, const std::string& new_path) {
    return Status::NotSupported("StarletFileSystem::link_file");
}

Status StarletFileSystem::drop_local_cache(const std::string& path) {
    auto full_path = relative_path(path);
    return StarletFileSystem::from_absl_status(_fs->drop_cache(full_path));
}

std::string StarletFileSystem::root_dir() {
    return _root_path;
}

std::string StarletFileSystem::full_path(std::string_view path) {
    return join_path(_root_path, path);
}

std::string StarletFileSystem::join_path(std::string_view part1, std::string_view part2) {
    assert(!part1.empty());
    assert(!part2.empty());
    assert(part2.back() != '/');
    if (part1.back() != '/') {
        return fmt::format("{}/{}", part1, part2);
    }
    return fmt::format("{}{}", part1, part2);
}

Status StarletFileSystem::from_absl_status(absl::Status absl_st) {
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

} // namespace starrocks

#endif // USE_STAROS
