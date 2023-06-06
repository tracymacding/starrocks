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

#include "common/compiler_util.h"

DIAGNOSTIC_PUSH
DIAGNOSTIC_IGNORE("-Wclass-memaccess")
#include <bvar/bvar.h>
DIAGNOSTIC_POP

#include <fmt/core.h>
#include <fslib/file.h>
#include <fslib/file_system.h>
#include <fslib/stat.h>
#include <fslib/stream.h>
#include <starlet.h>
#include <sys/stat.h>

#include "fs/output_stream_adapter.h"
#include "io/input_stream.h"
#include "io/output_stream.h"
#include "io/seekable_input_stream.h"

namespace starrocks {

using WriteOptions = staros::starlet::fslib::WriteOptions;
using ReadOptions = staros::starlet::fslib::ReadOptions;
using ReadOnlyFilePtr = std::unique_ptr<staros::starlet::fslib::ReadOnlyFile>;
using WritableFilePtr = std::unique_ptr<staros::starlet::fslib::WritableFile>;

class StarletInputStream : public starrocks::io::SeekableInputStream {
public:
    explicit StarletInputStream(ReadOnlyFilePtr file_ptr) : _file_ptr(std::move(file_ptr)){};
    ~StarletInputStream() = default;
    StarletInputStream(const StarletInputStream&) = delete;
    void operator=(const StarletInputStream&) = delete;
    StarletInputStream(StarletInputStream&&) = delete;
    void operator=(StarletInputStream&&) = delete;

    Status seek(int64_t position) override;

    StatusOr<int64_t> position() override;

    StatusOr<int64_t> get_size() override;

    StatusOr<int64_t> read(void* data, int64_t count) override;

    StatusOr<std::unique_ptr<io::NumericStatistics>> get_numeric_statistics() override;

private:
    ReadOnlyFilePtr _file_ptr;
};

class StarletOutputStream : public starrocks::io::OutputStream {
public:
    explicit StarletOutputStream(WritableFilePtr file_ptr) : _file_ptr(std::move(file_ptr)){};
    StarletOutputStream(const StarletOutputStream&) = delete;

    ~StarletOutputStream() = default;
  
    void operator=(const StarletOutputStream&) = delete;
    StarletOutputStream(StarletOutputStream&&) = delete;
    void operator=(StarletOutputStream&&) = delete;

    Status skip(int64_t count) override { return Status::NotSupported("StarletOutputStream::skip"); }
    
    StatusOr<Buffer> get_direct_buffer() override {
        return Status::NotSupported("StarletOutputStream::get_direct_buffer");
    }

    StatusOr<Position> get_direct_buffer_and_advance(int64_t size) override {
        return Status::NotSupported("StarletOutputStream::get_direct_buffer_and_advance");
    }

    Status write(const void* data, int64_t size) override;

    bool allows_aliasing() const override;

    Status write_aliased(const void* data, int64_t size) override;

    Status close() override;

private:
    WritableFilePtr _file_ptr;
};

class StarletFileSystem : public FileSystem {
public:
    StarletFileSystem(std::shared_ptr<staros::starlet::fslib::FileSystem> fs, std::string starlet_prefix, std::string root_path)
        : _fs(fs),
          _starlet_prefix(starlet_prefix),
          _root_path(root_path) {}

    ~StarletFileSystem() override = default;

    StarletFileSystem(const StarletFileSystem&) = delete;
    void operator=(const StarletFileSystem&) = delete;
    StarletFileSystem(StarletFileSystem&&) = delete;
    void operator=(StarletFileSystem&&) = delete;

    Type type() const override { return STARLET; }

    Status canonicalize(const std::string& path, std::string* result) override;

    std::string normalize_path(std::string_view starlet_file_path);

    std::string relative_path(std::string_view starlet_file_path);

    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const RandomAccessFileOptions& opts,
                                                                       const std::string& path) override;

    StatusOr<std::unique_ptr<SequentialFile>> new_sequential_file(const SequentialFileOptions& opts,
                                                                  const std::string& path) override;

    StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const std::string& path);

    StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const WritableFileOptions& opts,
                                                              const std::string& path) override;

    Status delete_file(const std::string& path) override;

    Status iterate_dir(const std::string& dir, const std::function<bool(std::string_view)>& cb) override;

    Status iterate_dir2(const std::string& dir, const std::function<bool(DirEntry)>& cb) override;

    Status create_dir(const std::string& dirname) override;

    Status create_dir_if_missing(const std::string& dirname, bool* created) override;

    Status create_dir_recursive(const std::string& dirname) override;

    Status delete_dir(const std::string& dirname) override;

    Status delete_dir_recursive(const std::string& dirname) override;

    // determine given path is a directory or not
    // returns
    //  * true - exists and is a directory
    //  * false - exists but is not a directory
    //  * error status: not exist or other errors
    StatusOr<bool> is_directory(const std::string& path) override;

    Status sync_dir(const std::string& dirname) override;

    StatusOr<SpaceInfo> space(const std::string& path) override;

    Status path_exists(const std::string& path) override;

    Status get_children(const std::string& dir, std::vector<std::string>* file) override;

    StatusOr<uint64_t> get_file_size(const std::string& path) override;

    StatusOr<uint64_t> get_file_modified_time(const std::string& path) override;

    Status rename_file(const std::string& src, const std::string& target) override;

    Status link_file(const std::string& old_path, const std::string& new_path) override;

    Status drop_local_cache(const std::string& path) override;

    std::string root_dir() override;

    // return full path for starlet file system
    std::string full_path(std::string_view path) override;

    // join two part to one path
    std::string join_path(std::string_view part1, std::string_view part2) override;

    static Status from_absl_status(absl::Status absl_st);

private:
    std::shared_ptr<staros::starlet::fslib::FileSystem> _fs;
    std::string _starlet_prefix;
    std::string _root_path;
};

} // namespace starrocks

#endif // USE_STAROS
