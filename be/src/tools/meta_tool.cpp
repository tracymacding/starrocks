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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/tools/meta_tool.cpp

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

#include <aws/core/Aws.h>
#include <fmt/format.h>
#include <gflags/gflags.h>

#include <iostream>
#include <set>
#include <string>

#include "column/datum_convert.h"
#include "common/status.h"
#include "fs/fs.h"
#include "fs/fs_posix.h"
#include "fs/fs_s3.h"
#include "fs/fs_util.h"
#include "gen_cpp/lake_types.pb.h"
#include "gen_cpp/olap_file.pb.h"
#include "gen_cpp/segment.pb.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/split.h"
#include "gutil/strings/substitute.h"
#include "json2pb/pb_to_json.h"
#include "storage/chunk_helper.h"
#include "storage/data_dir.h"
#include "storage/delta_column_group.h"
#include "storage/key_coder.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/vacuum.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/options.h"
#include "storage/primary_key_dump.h"
#include "storage/rowset/binary_plain_page.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/column_reader.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_options.h"
#include "storage/tablet_meta.h"
#include "storage/tablet_meta_manager.h"
#include "storage/tablet_schema_map.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/path_util.h"

using starrocks::DataDir;
using starrocks::KVStore;
using starrocks::Status;
using starrocks::TabletMeta;
using starrocks::TabletMetaManager;
using starrocks::MetaStoreStats;
using starrocks::Slice;
using starrocks::RandomAccessFile;
using starrocks::MemTracker;
using strings::Substitute;
using starrocks::SegmentFooterPB;
using starrocks::ColumnReader;
using starrocks::BinaryPlainPageDecoder;
using starrocks::PageHandle;
using starrocks::PagePointer;
using starrocks::ColumnIteratorOptions;
using starrocks::PageFooterPB;
using starrocks::DeltaColumnGroupList;
using starrocks::PrimaryKeyDump;

DEFINE_string(root_path, "", "storage root path");
DEFINE_string(operation, "",
              "valid operation: get_meta, flag, load_meta, delete_meta, delete_rowset_meta, get_persistent_index_meta, "
              "delete_persistent_index_meta, show_meta, check_table_meta_consistency, print_lake_metadata, "
              "print_lake_bundle_metadata, print_lake_txn_log, print_lake_schema");
DEFINE_int64(tablet_id, 0, "tablet_id for tablet meta");
DEFINE_string(tablet_uid, "", "tablet_uid for tablet meta");
DEFINE_int64(table_id, 0, "table id for table meta");
DEFINE_string(rowset_id, "", "rowset_id");
DEFINE_int32(schema_hash, 0, "schema_hash for tablet meta");
DEFINE_string(json_meta_path, "", "absolute json meta file path");
DEFINE_string(pb_meta_path, "", "pb meta file path");
DEFINE_string(tablet_file, "", "file to save a set of tablets");
DEFINE_string(file, "", "segment file path");
DEFINE_int32(column_index, -1, "column index");
DEFINE_int32(key_column_count, 0, "key column count");
DEFINE_int64(expired_sec, 86400, "expired seconds");
DEFINE_string(conf_file, "", "conf file path");
DEFINE_string(audit_file, "", "audit file path");
DEFINE_bool(do_delete, false, "do delete files");

// flag defined in gflags library
DECLARE_bool(helpshort);

std::string get_usage(const std::string& progname) {
    constexpr const char* const usage_msg = R"(
  {progname} is the StarRocks BE Meta tool.
    [CAUTION] Stop BE first before using this tool if modification will be made.

  Usage:
    get_meta:
      {progname} --operation=get_meta --root_path=</path/to/storage/path> --tablet_id=<tabletid> [--schema_hash=<schemahash>]
    load_meta:
      {progname} --operation=load_meta --root_path=</path/to/storage/path> --json_meta_path=<path>
    delete_meta:
      {progname} --operation=delete_meta --root_path=</path/to/storage/path> --tablet_id=<tabletid> [--schema_hash=<schemahash>]
      {progname} --operation=delete_meta --root_path=</path/to/storage/path> --table_id=<tableid>
      {progname} --operation=delete_meta --tablet_file=<file_path>
    delete_rowset_meta:
      {progname} --operation=delete_rowset_meta --root_path=</path/to/storage/path> --tablet_uid=<tablet_uid> --rowset_id=<rowset_id>
    get_persistent_index_meta:
      {progname} --operation=get_persistent_index_meta --root_path=</path/to/storage/path> --tablet_id=<tabletid>
    delete_persistent_index_meta:
      {progname} --operation=delete_persistent_index_meta --root_path=</path/to/storage/path> --tablet_id=<tabletid>
      {progname} --operation=delete_persistent_index_meta --root_path=</path/to/storage/path> --table_id=<tableid>
    compact_meta:
      {progname} --operation=compact_meta --root_path=</path/to/storage/path>
    get_meta_stats:
      {progname} --operation=get_meta_stats --root_path=</path/to/storage/path>
    ls:
      {progname} --operation=ls --root_path=</path/to/storage/path>
    show_meta:
      {progname} --operation=show_meta --pb_meta_path=<path>
    show_segment_footer:
      {progname} --operation=show_segment_footer --file=</path/to/segment/file>
    dump_segment_data:
      {progname} --operation=dump_segment_data --file=</path/to/segment/file>
    dump_column_size:
      {progname} --operation=dump_column_size --file=</path/to/segment/file>
    print_pk_dump:
      {progname} --operation=print_pk_dump --file=</path/to/pk/dump/file>
    dump_short_key_index:
      {progname} --operation=dump_short_key_index --file=</path/to/segment/file> --key_column_count=<2>
    calc_checksum:
      {progname} --operation=calc_checksum [--column_index=<index>] --file=</path/to/segment/file>
    check_table_meta_consistency:
      {progname} --operation=check_table_meta_consistency --root_path=</path/to/storage/path> --table_id=<tableid>
    scan_dcgs:
      {progname} --operation=scan_dcgs --root_path=</path/to/storage/path> --tablet_id=<tabletid>
    print_lake_metadata:
      cat <tablet_meta_file.meta> | {progname} --operation=print_lake_metadata
    print_lake_bundle_metadata:
      cat <tablet_meta_file.meta> | {progname} --operation=print_lake_bundle_metadata
    print_lake_txn_log:
      cat <tablet_transaction_log_file.log> | {progname} --operation=print_lake_txn_log
    print_lake_schema:
      cat <tablet_schema_file> | {progname} --operation=print_lake_schema
    lake_datafile_gc:
      {progname} --operation=lake_datafile_gc --root_path=<path> --expired_sec=<86400> --conf_file=<path> --audit_file=<path> --do_delete=<true|false>
    )";
    return fmt::format(usage_msg, fmt::arg("progname", progname));
}

static void show_usage() {
    FLAGS_helpshort = true;
    google::HandleCommandLineHelpFlags();
}

void show_meta() {
    TabletMeta tablet_meta;
    Status s = tablet_meta.create_from_file(FLAGS_pb_meta_path);
    if (!s.ok()) {
        std::cout << "load pb meta file:" << FLAGS_pb_meta_path << " failed"
                  << ", status:" << s << std::endl;
        return;
    }
    std::string json_meta;
    json2pb::Pb2JsonOptions json_options;
    json_options.pretty_json = true;
    starrocks::TabletMetaPB tablet_meta_pb;
    tablet_meta.to_meta_pb(&tablet_meta_pb);
    json2pb::ProtoMessageToJson(tablet_meta_pb, &json_meta, json_options);
    std::cout << json_meta << std::endl;
}

void get_meta(DataDir* data_dir) {
    std::string value;
    if (FLAGS_schema_hash != 0) {
        auto s = TabletMetaManager::get_json_meta(data_dir, FLAGS_tablet_id, FLAGS_schema_hash, &value);
        if (s.is_not_found()) {
            std::cout << "no tablet meta for tablet_id:" << FLAGS_tablet_id << ", schema_hash:" << FLAGS_schema_hash
                      << std::endl;
            return;
        } else if (!s.ok()) {
            std::cerr << "fail to get tablet meta: " << s << std::endl;
        }
    } else {
        auto s = TabletMetaManager::get_json_meta(data_dir, FLAGS_tablet_id, &value);
        if (!s.ok()) {
            if (s.is_not_found()) {
                std::cout << "no tablet meta for tablet_id:" << FLAGS_tablet_id << std::endl;
            } else {
                std::cout << "get tablet meta failed: " << s.to_string();
            }
            return;
        }
    }
    std::cout << value << std::endl;
}

void load_meta(DataDir* data_dir) {
    // load json tablet meta into meta
    Status s = TabletMetaManager::load_json_meta(data_dir, FLAGS_json_meta_path);
    if (!s.ok()) {
        std::cout << "load meta failed, status:" << s << std::endl;
        return;
    }
    std::cout << "load meta successfully" << std::endl;
}

void delete_meta(DataDir* data_dir) {
    if (FLAGS_table_id != 0) {
        auto st = TabletMetaManager::remove_table_meta(data_dir, FLAGS_table_id);
        if (!st.ok()) {
            std::cout << "delete table meta failed for table_id:" << FLAGS_table_id << ", status:" << st << std::endl;
            return;
        }
    } else if (FLAGS_schema_hash != 0) {
        auto st = TabletMetaManager::remove(data_dir, FLAGS_tablet_id, FLAGS_schema_hash);
        if (!st.ok()) {
            std::cout << "delete tablet meta failed for tablet_id:" << FLAGS_tablet_id
                      << ", schema_hash:" << FLAGS_schema_hash << ", status:" << st << std::endl;
            return;
        }
    } else {
        auto st = TabletMetaManager::remove(data_dir, FLAGS_tablet_id);
        if (!st.ok()) {
            std::cout << "delete tablet meta failed for tablet_id:" << FLAGS_tablet_id << " status:" << st.to_string()
                      << std::endl;
            return;
        }
    }
    std::cout << "delete meta successfully" << std::endl;
}

void delete_rowset_meta(DataDir* data_dir) {
    std::string key = "rst_" + FLAGS_tablet_uid + "_" + FLAGS_rowset_id;
    Status s = data_dir->get_meta()->remove(starrocks::META_COLUMN_FAMILY_INDEX, key);
    if (!s.ok()) {
        std::cout << "delete rowset meta failed for tablet_uid:" << FLAGS_tablet_uid
                  << ", rowset_id:" << FLAGS_rowset_id << ", status:" << s << std::endl;
        return;
    }
    std::cout << "delete rowset meta successfully" << std::endl;
}

void get_persistent_index_meta(DataDir* data_dir) {
    starrocks::PersistentIndexMetaPB index_meta;
    auto st = TabletMetaManager::get_persistent_index_meta(data_dir, FLAGS_tablet_id, &index_meta);
    if (!st.ok()) {
        std::cerr << "get persistent index meta failed for tablet: " << FLAGS_tablet_id
                  << ", status: " << st.to_string() << std::endl;
        return;
    }
    json2pb::Pb2JsonOptions options;
    options.pretty_json = true;
    std::string json;
    std::string error;
    if (!json2pb::ProtoMessageToJson(index_meta, &json, options, &error)) {
        std::cerr << "Fail to convert protobuf to json: " << error << std::endl;
        return;
    }
    std::cout << json << '\n';
}

void delete_persistent_index_meta(DataDir* data_dir) {
    if (FLAGS_table_id != 0) {
        auto st = TabletMetaManager::remove_table_persistent_index_meta(data_dir, FLAGS_table_id);
        if (!st.ok()) {
            std::cout << "delete table persistent index meta failed for table_id:" << FLAGS_table_id
                      << " status:" << st.to_string() << std::endl;
            return;
        }
        std::cout << "delete table persistent index meta successfully" << std::endl;
    } else {
        std::string key = "tpi_";
        starrocks::put_fixed64_le(&key, BigEndian::FromHost64(FLAGS_tablet_id));
        Status st = data_dir->get_meta()->remove(starrocks::META_COLUMN_FAMILY_INDEX, key);
        if (st.ok()) {
            std::cout << "delete tablet persistent index meta success, tablet_id: " << FLAGS_tablet_id << std::endl;
        } else {
            std::cout << "delete tablet persistent index meta failed, tablet_id: " << FLAGS_tablet_id
                      << ", status: " << st.to_string() << std::endl;
        }
    }
}

void compact_meta(DataDir* data_dir) {
    uint64_t live_sst_files_size_before = 0;
    uint64_t live_sst_files_size_after = 0;
    if (!data_dir->get_meta()->get_live_sst_files_size(&live_sst_files_size_before)) {
        std::cout << "data dir " << data_dir->path() << " get_live_sst_files_size failed" << std::endl;
    }
    auto s = data_dir->get_meta()->compact();
    if (!s.ok()) {
        std::cout << "data dir " << data_dir->path() << " compact meta failed: " << s << std::endl;
        return;
    }
    if (!data_dir->get_meta()->get_live_sst_files_size(&live_sst_files_size_after)) {
        std::cout << "data dir " << data_dir->path() << " get_live_sst_files_size failed" << std::endl;
    }
    std::cout << "data dir " << data_dir->path() << " compact meta successfully, "
              << "live_sst_files_size_before: " << live_sst_files_size_before
              << " live_sst_files_size_after: " << live_sst_files_size_after << data_dir->get_meta()->get_stats()
              << std::endl;
}

void get_meta_stats(DataDir* data_dir) {
    MetaStoreStats stats;
    auto st = TabletMetaManager::get_stats(data_dir, &stats, false);
    if (!st.ok()) {
        std::cout << "get_meta_stats failed: " << st.to_string() << std::endl;
        return;
    }
    printf("Non-update tablets:\n");
    printf("         tablet: %8zu %10zu\n", stats.tablet_count, stats.tablet_meta_bytes);
    printf("            rst: %8zu %10zu\n", stats.rowset_count, stats.rowset_meta_bytes);
    printf("Update tablets:\n");
    printf("         tablet: %8zu %10zu\n", stats.update_tablet_count, stats.update_tablet_meta_bytes);
    printf("            log: %8zu %10zu\n", stats.log_count, stats.log_meta_bytes);
    printf("  delete vector: %8zu %10zu\n", stats.delvec_count, stats.delvec_meta_bytes);
    printf("         rowset: %8zu %10zu\n", stats.update_rowset_count, stats.update_rowset_meta_bytes);
    printf(" pending rowset: %8zu %10zu\n", stats.pending_rowset_count, stats.pending_rowset_count);
    printf("\n          Total: %8zu %10zu\n", stats.total_count, stats.total_meta_bytes);
    printf("Error: %zu\n", stats.error_count);
}

void list_meta(DataDir* data_dir) {
    MetaStoreStats stats;
    auto st = TabletMetaManager::get_stats(data_dir, &stats, true);
    if (!st.ok()) {
        std::cout << "list_meta: " << st.to_string() << std::endl;
        return;
    }
    printf("%8s %8s %18s %4s %16s %8s %18s %6s %18s %18s %26s\n", "table", "tablet", "tablet_meta_bytes", "log",
           "log_meta_bytes", "delvec", "delvec_meta_bytes", "rowset", "rowset_meta_bytes", "pending_rowset",
           "pending_rowset_meta_bytes");
    for (auto& e : stats.tablets) {
        auto& st = e.second;
        printf("%8ld %8ld %18zu %4zu %16zu %8zu %18zu %6zu %18lu %18lu %26lu\n", st.table_id, st.tablet_id,
               st.tablet_meta_bytes, st.log_count, st.log_meta_bytes, st.delvec_count, st.delvec_meta_bytes,
               st.rowset_count, st.rowset_meta_bytes, st.pending_rowset_count, st.pending_rowset_meta_bytes);
    }
    printf("  Total KV: %zu Bytes: %zu Tablets: %zu (PK: %zu Other: %zu) Error: %zu\n", stats.total_count,
           stats.total_meta_bytes, stats.tablets.size(), stats.update_tablet_count, stats.tablet_count,
           stats.error_count);
}

Status init_data_dir(const std::string& dir, std::unique_ptr<DataDir>* ret, bool read_only = false) {
    std::string root_path;
    Status st = starrocks::fs::canonicalize(dir, &root_path);
    if (!st.ok()) {
        std::cout << "invalid root path:" << FLAGS_root_path << ", error: " << st.to_string() << std::endl;
        return Status::InternalError("invalid root path");
    }
    starrocks::StorePath path;
    auto res = parse_root_path(root_path, &path);
    if (!res.ok()) {
        std::cout << "parse root path failed:" << root_path << std::endl;
        return res;
    }

    std::unique_ptr<DataDir> p(new (std::nothrow) DataDir(path.path, path.storage_medium));
    if (p == nullptr) {
        std::cout << "new data dir failed" << std::endl;
        return Status::InternalError("new data dir failed");
    }
    st = p->init(read_only);
    if (!st.ok()) {
        std::cout << "data_dir load failed" << std::endl;
        return Status::InternalError("data_dir load failed");
    }

    p.swap(*ret);
    return Status::OK();
}

void batch_delete_meta(const std::string& tablet_file) {
    // each line in tablet file indicate a tablet to delete, format is:
    //      data_dir,tablet_id,schema_hash
    // eg:
    //      /data1/starrocks.HDD,100010,11212389324
    //      /data2/starrocks.HDD,100010,23049230234
    // Or:
    //      data_dir,tablet_id
    // eg:
    //      /data1/starrocks.HDD,100010
    //      /data2/starrocks.HDD,100010
    std::ifstream infile(tablet_file);
    std::string line;
    int err_num = 0;
    int delete_num = 0;
    int total_num = 0;
    std::unordered_map<std::string, std::unique_ptr<DataDir>> dir_map;
    while (std::getline(infile, line)) {
        total_num++;
        std::vector<string> v = strings::Split(line, ",");
        if (!(v.size() == 2 || v.size() == 3)) {
            std::cout << "invalid line in tablet_file: " << line << std::endl;
            err_num++;
            continue;
        }
        // 1. get dir
        std::string dir;
        Status st = starrocks::fs::canonicalize(v[0], &dir);
        if (!st.ok()) {
            std::cout << "invalid root dir in tablet_file: " << line << std::endl;
            err_num++;
            continue;
        }

        if (dir_map.find(dir) == dir_map.end()) {
            // new data dir, init it
            std::unique_ptr<DataDir> data_dir_p;
            Status st = init_data_dir(dir, &data_dir_p);
            if (!st.ok()) {
                std::cout << "invalid root path:" << FLAGS_root_path << ", error: " << st.to_string() << std::endl;
                err_num++;
                continue;
            }
            dir_map[dir] = std::move(data_dir_p);
            std::cout << "get a new data dir: " << dir << std::endl;
        }
        DataDir* data_dir = dir_map[dir].get();
        if (data_dir == nullptr) {
            std::cout << "failed to get data dir: " << line << std::endl;
            err_num++;
            continue;
        }

        // 2. get tablet id/schema_hash
        int64_t tablet_id;
        if (!safe_strto64(v[1].c_str(), &tablet_id)) {
            std::cout << "invalid tablet id: " << line << std::endl;
            err_num++;
            continue;
        }
        if (v.size() == 3) {
            int64_t schema_hash;
            if (!safe_strto64(v[2].c_str(), &schema_hash)) {
                std::cout << "invalid schema hash: " << line << std::endl;
                err_num++;
                continue;
            }

            Status s = TabletMetaManager::remove(data_dir, tablet_id, schema_hash);
            if (!s.ok()) {
                std::cout << "delete tablet meta failed for tablet_id:" << tablet_id << ", schema_hash:" << schema_hash
                          << ", status:" << s << std::endl;
                err_num++;
                continue;
            }
        } else {
            auto s = TabletMetaManager::remove(data_dir, tablet_id);
            if (!s.ok()) {
                std::cout << "delete tablet meta failed for tablet_id:" << tablet_id << ", status:" << s.to_string()
                          << std::endl;
                err_num++;
                continue;
            }
        }

        delete_num++;
    }

    std::cout << "total: " << total_num << ", delete: " << delete_num << ", error: " << err_num << std::endl;
}

Status get_segment_footer(RandomAccessFile* input_file, SegmentFooterPB* footer) {
    // Footer := SegmentFooterPB, FooterPBSize(4), FooterPBChecksum(4), MagicNumber(4)
    const std::string& file_name = input_file->filename();
    ASSIGN_OR_RETURN(const uint64_t file_size, input_file->get_size());

    if (file_size < 12) {
        return Status::Corruption(strings::Substitute("Bad segment file $0: file size $1 < 12", file_name, file_size));
    }

    uint8_t fixed_buf[12];
    RETURN_IF_ERROR(input_file->read_at_fully(file_size - 12, fixed_buf, 12));

    // validate magic number
    const char* k_segment_magic = "D0R1";
    const uint32_t k_segment_magic_length = 4;
    if (memcmp(fixed_buf + 8, k_segment_magic, k_segment_magic_length) != 0) {
        return Status::Corruption(strings::Substitute("Bad segment file $0: magic number not match", file_name));
    }

    // read footer PB
    uint32_t footer_length = starrocks::decode_fixed32_le(fixed_buf);
    if (file_size < 12 + footer_length) {
        return Status::Corruption(strings::Substitute("Bad segment file $0: file size $1 < $2", file_name, file_size,
                                                      12 + footer_length));
    }
    std::string footer_buf;
    footer_buf.resize(footer_length);
    RETURN_IF_ERROR(input_file->read_at_fully(file_size - 12 - footer_length, footer_buf.data(), footer_buf.size()));

    // validate footer PB's checksum
    uint32_t expect_checksum = starrocks::decode_fixed32_le(fixed_buf + 4);
    uint32_t actual_checksum = starrocks::crc32c::Value(footer_buf.data(), footer_buf.size());
    if (actual_checksum != expect_checksum) {
        return Status::Corruption(
                strings::Substitute("Bad segment file $0: footer checksum not match, actual=$1 vs expect=$2", file_name,
                                    actual_checksum, expect_checksum));
    }

    // deserialize footer PB
    if (!footer->ParseFromString(footer_buf)) {
        return Status::Corruption(
                strings::Substitute("Bad segment file $0: failed to parse SegmentFooterPB", file_name));
    }
    return Status::OK();
}

void show_segment_footer(const std::string& file_name) {
    auto res = starrocks::FileSystem::Default()->new_random_access_file(file_name);
    if (!res.ok()) {
        std::cout << "open file failed: " << res.status() << std::endl;
        return;
    }
    auto input_file = std::move(res).value();
    SegmentFooterPB footer;
    auto status = get_segment_footer(input_file.get(), &footer);
    if (!status.ok()) {
        std::cout << "get footer failed: " << status.to_string() << std::endl;
        return;
    }
    std::string json_footer;
    json2pb::Pb2JsonOptions json_options;
    json_options.pretty_json = true;
    bool ret = json2pb::ProtoMessageToJson(footer, &json_footer, json_options);
    if (!ret) {
        std::cout << "Convert PB to json failed" << std::endl;
        return;
    }
    std::cout << json_footer << std::endl;
}

// This function will check the consistency of tablet meta and segment_footer
// #issue 5415
void check_meta_consistency(DataDir* data_dir) {
    std::vector<int64_t> tablet_ids;
    int64_t table_id = FLAGS_table_id;
    auto check_meta_func = [data_dir, &tablet_ids, table_id](int64_t tablet_id, int32_t schema_hash,
                                                             std::string_view value) -> bool {
        starrocks::TabletMetaSharedPtr tablet_meta(new TabletMeta());
        // if deserialize failed, skip it
        if (Status st = tablet_meta->deserialize(value); !st.ok()) {
            return true;
        }
        // tablet is not belong to the table, skip it
        if (tablet_meta->table_id() != table_id) {
            return true;
        }
        std::string tablet_path = data_dir->path() + starrocks::DATA_PREFIX;
        tablet_path = starrocks::path_util::join_path_segments(tablet_path, std::to_string(tablet_meta->shard_id()));
        tablet_path = starrocks::path_util::join_path_segments(tablet_path, std::to_string(tablet_meta->tablet_id()));
        tablet_path = starrocks::path_util::join_path_segments(tablet_path, std::to_string(tablet_meta->schema_hash()));

        auto tablet_schema = tablet_meta->tablet_schema_ptr();
        const std::vector<starrocks::TabletColumn>& columns = tablet_schema->columns();

        for (const auto& rs : tablet_meta->all_rs_metas()) {
            for (int64_t seg_id = 0; seg_id < rs->num_segments(); ++seg_id) {
                std::string seg_path =
                        strings::Substitute("$0/$1_$2.dat", tablet_path, rs->rowset_id().to_string(), seg_id);
                auto res = starrocks::FileSystem::Default()->new_random_access_file(seg_path);
                if (!res.ok()) {
                    continue;
                }
                auto seg_file = std::move(res).value();
                starrocks::SegmentFooterPB footer;
                res = get_segment_footer(seg_file.get(), &footer);
                if (!res.ok()) {
                    continue;
                }

                // unique_id: ordinal: column_type
                std::unordered_map<uint32_t, std::pair<uint32_t, int32_t>> columns_in_footer;
                for (uint32_t ordinal = 0; ordinal < footer.columns().size(); ++ordinal) {
                    const auto& column_pb = footer.columns(ordinal);
                    columns_in_footer.emplace(column_pb.unique_id(), std::make_pair(ordinal, column_pb.type()));
                }
                for (const auto& column : columns) {
                    uint32_t unique_id = column.unique_id();
                    starrocks::LogicalType type = column.type();
                    auto iter = columns_in_footer.find(unique_id);
                    if (iter == columns_in_footer.end()) {
                        continue;
                    }

                    // find a segment inconsistency, return directly
                    if (iter->second.second != type) {
                        tablet_ids.emplace_back(tablet_id);
                        return true;
                    }

                    // if type is varchar, check length
                    if (type == starrocks::LogicalType::TYPE_VARCHAR) {
                        const auto& column_pb = footer.columns(iter->second.first);
                        if (column.length() != column_pb.length()) {
                            tablet_ids.emplace_back(tablet_id);
                            return true;
                        }
                    }
                }
            }
        }
        return true;
    };
    Status load_tablet_status = TabletMetaManager::walk(data_dir->get_meta(), check_meta_func);
    if (tablet_ids.size() > 0) {
        std::cout << "inconsistency tablet:";
    }
    for (long tablet_id : tablet_ids) {
        std::cout << "," << tablet_id;
    }
    return;
}

void scan_dcgs(DataDir* data_dir) {
    DeltaColumnGroupList dcgs;
    Status st = TabletMetaManager::scan_tablet_delta_column_group(data_dir->get_meta(), FLAGS_tablet_id, &dcgs);
    if (!st.ok()) {
        std::cout << "scan delta column group, st: " << st.to_string() << std::endl;
        return;
    }
    for (const auto& dcg : dcgs) {
        std::cout << dcg->debug_string() << std::endl;
    }
}

namespace starrocks {

class SegmentDump {
public:
    SegmentDump(std::string path, int32_t column_index = -1) : _path(std::move(path)), _column_index(column_index) {}
    ~SegmentDump() = default;

    Status dump_segment_data();
    Status dump_short_key_index(size_t key_column_count);
    Status calc_checksum();
    Status dump_column_size();

private:
    struct ColItem {
        TypeInfoPtr type;
        size_t offset;
        size_t size;
    };

    Status _init();
    void _convert_column_meta(const ColumnMetaPB& src_col, ColumnPB* dest_col);
    std::shared_ptr<Schema> _init_query_schema(const std::shared_ptr<TabletSchema>& tablet_schema);
    std::shared_ptr<Schema> _init_query_schema_by_column_id(const std::shared_ptr<TabletSchema>& tablet_schema,
                                                            ColumnId id);
    std::shared_ptr<TabletSchema> _init_search_schema_from_footer(const SegmentFooterPB& footer);
    void _analyze_short_key_columns(size_t key_column_count, std::vector<ColItem>* cols);
    Status _output_short_key_string(const std::vector<ColItem>& cols, size_t idx, Slice& key, std::string* result);

    std::shared_ptr<FileSystem> _fs;
    std::unique_ptr<RandomAccessFile> _input_file;
    std::string _path;
    std::shared_ptr<Segment> _segment;
    std::shared_ptr<TabletSchema> _tablet_schema;
    SegmentFooterPB _footer;
    MemPool _mem_pool;
    const size_t _max_short_key_size = 36;
    const size_t _max_short_key_col_cnt = 3;
    int32_t _column_index = 0;
};

std::shared_ptr<Schema> SegmentDump::_init_query_schema(const std::shared_ptr<TabletSchema>& tablet_schema) {
    return std::make_shared<Schema>(tablet_schema->schema());
}

std::shared_ptr<Schema> SegmentDump::_init_query_schema_by_column_id(const std::shared_ptr<TabletSchema>& tablet_schema,
                                                                     ColumnId id) {
    std::vector<ColumnId> cids;
    cids.push_back(id);
    return std::make_shared<Schema>(tablet_schema->schema(), cids);
}

void SegmentDump::_convert_column_meta(const ColumnMetaPB& src_col, ColumnPB* dest_col) {
    dest_col->set_unique_id(src_col.unique_id());
    dest_col->set_type(type_to_string(LogicalType(src_col.type())));
    dest_col->set_is_nullable(src_col.is_nullable());
    dest_col->set_length(src_col.length());
    dest_col->set_name(src_col.name());

    const auto& src_child_cols = src_col.children_columns();
    for (const auto& src_child_col : src_child_cols) {
        auto* dest_child_col = dest_col->add_children_columns();
        _convert_column_meta(src_child_col, dest_child_col);
    }
}

std::shared_ptr<TabletSchema> SegmentDump::_init_search_schema_from_footer(const SegmentFooterPB& footer) {
    TabletSchemaPB tablet_schema_pb;
    for (int i = 0; i < footer.columns_size(); i++) {
        const auto& src_col = footer.columns(i);
        ColumnPB* dest_col = tablet_schema_pb.add_column();
        _convert_column_meta(src_col, dest_col);
    }

    return std::make_shared<TabletSchema>(tablet_schema_pb);
}

Status SegmentDump::_init() {
    // open file
    _fs = new_fs_posix();

    auto res = _fs->new_random_access_file(_path);
    if (!res.ok()) {
        std::cout << "open file failed: " << res.status() << std::endl;
        return Status::InternalError("");
    }
    _input_file = std::move(res.value());

    // parse segment footer
    Status st = get_segment_footer(_input_file.get(), &_footer);
    if (!st.ok()) {
        std::cout << "parse segment footer failed: " << st << std::endl;
        return Status::InternalError("");
    }

    // construct tablet schema
    _tablet_schema = _init_search_schema_from_footer(_footer);

    // open segment
    size_t footer_length = 16 * 1024 * 1024;
    auto segment_res = Segment::open(_fs, FileInfo{_path}, 0, _tablet_schema, &footer_length, nullptr);
    if (!segment_res.ok()) {
        std::cout << "open segment failed: " << segment_res.status() << std::endl;
        return Status::InternalError("");
    }
    _segment = std::move(segment_res.value());

    return Status::OK();
}

void SegmentDump::_analyze_short_key_columns(size_t key_column_count, std::vector<ColItem>* cols) {
    size_t start_offset = 1;
    size_t short_key_size = 0;

    for (size_t i = 0; i < key_column_count; i++) {
        auto col = _tablet_schema->columns()[i];
        LogicalType logical_type = col.type();
        if (is_enumeration_type(logical_type)) {
            if (short_key_size + col.length() > _max_short_key_size) {
                break;
            }
            short_key_size += col.length();

            ColItem item;
            item.type = get_type_info(logical_type);
            item.offset = start_offset;
            item.size = item.type->size();
            cols->emplace_back(item);

            start_offset += item.type->size() + 1;
        } else {
            ColItem item;
            item.type = get_type_info(logical_type);
            item.offset = start_offset;
            item.size = 0;
            cols->emplace_back(item);

            break;
        }
    }
}

Status SegmentDump::_output_short_key_string(const std::vector<ColItem>& cols, size_t idx, Slice& key,
                                             std::string* result) {
    size_t item_size = cols[idx].size;
    if (item_size == 0) {
        item_size = key.size - cols[idx].offset;
    }
    Slice convert_key = {key.data + cols[idx].offset, item_size};

    size_t num_short_key_columns = cols.size();
    const KeyCoder* coder = get_key_coder(cols[idx].type->type());
    uint8_t* tmp_mem = _mem_pool.allocate(item_size);
    (void)coder->decode_ascending(&convert_key, item_size, tmp_mem, &_mem_pool);

    auto logical_type = cols[idx].type->type();

    switch (logical_type) {
#define M(logical_type)                                                                                   \
    case logical_type: {                                                                                  \
        Datum data;                                                                                       \
        data.set<TypeTraits<logical_type>::CppType>(*(TypeTraits<logical_type>::CppType*)(tmp_mem));      \
        result->append(" key");                                                                           \
        result->append(std::to_string(idx));                                                              \
        result->append("(");                                                                              \
        result->append(std::to_string(static_cast<int32_t>(*(uint8*)(key.data + cols[idx].offset - 1)))); \
        result->append(":");                                                                              \
        result->append(datum_to_string(cols[idx].type.get(), data));                                      \
        result->append(")");                                                                              \
        if (idx + 1 < num_short_key_columns) {                                                            \
            result->append(",");                                                                          \
        }                                                                                                 \
        break;                                                                                            \
    }
        APPLY_FOR_TYPE_INTEGER(M)
        APPLY_FOR_TYPE_TIME(M)
        APPLY_FOR_TYPE_DECIMAL(M)
        M(TYPE_FLOAT)
        M(TYPE_DOUBLE)
        M(TYPE_CHAR)
        M(TYPE_VARCHAR)
#undef M
    default:
        std::cout << "Not support type: " << logical_type << std::endl;
        return Status::InternalError("Not support type");
    }

    return Status::OK();
}

Status SegmentDump::calc_checksum() {
    Status st = _init();
    if (!st.ok()) {
        std::cout << "SegmentDump init failed: " << st << std::endl;
        return st;
    }

    // convert schema
    std::vector<uint32_t> return_columns;
    if (_column_index == -1) {
        size_t num_columns = _tablet_schema->num_columns();
        for (size_t i = 0; i < num_columns; i++) {
            LogicalType type = _tablet_schema->column(i).type();
            if (is_support_checksum_type(type)) {
                return_columns.push_back(i);
            }
        }
    } else {
        if (_column_index >= _tablet_schema->columns().size()) {
            LOG(INFO) << "this column is not exist: column_index=" << _column_index;
            return Status::OK();
        } else {
            LogicalType type = _tablet_schema->columns()[_column_index].type();
            if (is_support_checksum_type(type)) {
                return_columns.push_back(_column_index);
            }
        }
    }

    auto schema = ChunkHelper::convert_schema(_tablet_schema, return_columns);
    SegmentReadOptions seg_opts;
    seg_opts.fs = _fs;
    seg_opts.use_page_cache = false;
    OlapReaderStatistics stats;
    seg_opts.stats = &stats;
    auto seg_res = _segment->new_iterator(schema, seg_opts);
    if (!seg_res.ok()) {
        std::cout << "new segment iterator failed: " << seg_res.status().message() << std::endl;
        return seg_res.status();
    }
    auto seg_iter = std::move(seg_res.value());

    int64_t checksum = 0;

    auto chunk = ChunkHelper::new_chunk(schema, config::vector_chunk_size);
    st = seg_iter->get_next(chunk.get());
    while (st.ok()) {
        size_t size = chunk->num_rows();
        for (auto& column : chunk->columns()) {
            checksum ^= column->xor_checksum(0, size);
        }
        chunk->reset();
        st = seg_iter->get_next(chunk.get());
    }

    if (!st.is_end_of_file() && !st.ok()) {
        LOG(WARNING) << "Failed to do checksum. error:=" << st.to_string();
        return st;
    }

    LOG(INFO) << "success to finish compute checksum. checksum=" << (uint32_t)checksum;
    return Status::OK();
}

Status SegmentDump::dump_short_key_index(size_t key_column_count) {
    key_column_count = std::min(key_column_count, _max_short_key_col_cnt);
    Status st = _init();
    if (!st.ok()) {
        std::cout << "SegmentDump init failed: " << st << std::endl;
        return st;
    }

    st = _segment->load_index();
    if (!st.ok()) {
        std::cout << "load short key index failed: " << st << std::endl;
        return st;
    }

    const ShortKeyIndexDecoder* decoder = _segment->decoder();
    size_t key_count = decoder->num_items();
    std::cout << "Short key index items count: " << key_count << std::endl;
    std::cout << "MARKER: MIN(0x00), NULL_FIRST(0x01), NORMAL(0x02), NULL_LAST(0xFE), MAX(0xFF)" << std::endl;

    std::vector<ColItem> _cols;
    _analyze_short_key_columns(key_column_count, &_cols);

    for (size_t i = 0; i < key_count; i++) {
        Slice key = decoder->key(i);
        std::string result;

        for (size_t j = 0; j < _cols.size(); j++) {
            st = _output_short_key_string(_cols, j, key, &result);
            if (!st.ok()) {
                std::cout << "Output short key string failed: " << st << std::endl;
                return st;
            }
        }

        std::cout << "INDEX(" << i << "): " << result << std::endl;
    }

    return Status::OK();
}

Status SegmentDump::dump_segment_data() {
    Status st = _init();
    if (!st.ok()) {
        std::cout << "SegmentDump init failed: " << st << std::endl;
        return st;
    }

    // convert schema
    auto schema = _init_query_schema(_tablet_schema);
    SegmentReadOptions seg_opts;
    seg_opts.fs = _fs;
    seg_opts.use_page_cache = false;
    OlapReaderStatistics stats;
    seg_opts.stats = &stats;
    auto seg_res = _segment->new_iterator(*schema, seg_opts);
    if (!seg_res.ok()) {
        std::cout << "new segment iterator failed: " << seg_res.status() << std::endl;
        return seg_res.status();
    }
    auto seg_iter = std::move(seg_res.value());

    // iter chunk
    size_t row = 0;
    auto chunk = ChunkHelper::new_chunk(*schema, 4096);
    do {
        st = seg_iter->get_next(chunk.get());
        if (!st.ok()) {
            if (st.is_end_of_file()) {
                break;
            }
            std::cout << "iter chunk failed: " << st.to_string() << std::endl;
            return st;
        }

        for (size_t i = 0; i < chunk->num_rows(); i++) {
            std::cout << "ROW: (" << row << "): " << chunk->debug_row(i) << std::endl;
            row++;
        }
        chunk->reset();
    } while (true);

    return Status::OK();
}

Status SegmentDump::dump_column_size() {
    Status st = _init();
    if (!st.ok()) {
        std::cout << "SegmentDump init failed: " << st << std::endl;
        return st;
    }

    // for each column
    for (ColumnId id = 0; id < _tablet_schema->num_columns(); id++) {
        const ColumnMetaPB& column_meta = _footer.columns(id);
        auto& tablet_column = _tablet_schema->column(id);
        auto column_name = tablet_column.name();

        // read column one by one
        auto schema = _init_query_schema_by_column_id(_tablet_schema, id);
        SegmentReadOptions seg_opts;
        seg_opts.fs = _fs;
        seg_opts.use_page_cache = false;
        OlapReaderStatistics stats;
        seg_opts.stats = &stats;

        auto read_the_segment = [&]() {
            auto seg_res = _segment->new_iterator(*schema, seg_opts);
            if (!seg_res.ok()) {
                std::cout << "new segment iterator failed: " << seg_res.status() << std::endl;
                return seg_res.status();
            }
            auto seg_iter = std::move(seg_res.value());

            // iter chunk
            auto chunk = ChunkHelper::new_chunk(*schema, 4096);
            do {
                st = seg_iter->get_next(chunk.get());
                if (!st.ok()) {
                    if (st.is_end_of_file()) {
                        break;
                    }
                    std::cout << "iter chunk failed: " << st.to_string() << std::endl;
                    return st;
                }
                chunk->reset();
            } while (true);
            return Status::OK();
        };

        if (tablet_column.subcolumn_count() == 0) {
            // regular column
            read_the_segment().ok();

            auto compession_desc = CompressionTypePB_descriptor()->FindValueByNumber(column_meta.compression());
            auto encoding_desc = EncodingTypePB_descriptor()->FindValueByNumber(column_meta.encoding());

            fmt::print(
                    "[ column id: {} name: {} compression: {} encoding: {} compressed bytes: {} uncompressed "
                    "bytes: {}, rows: {}, pages: {}]\n",
                    id, column_name, compession_desc->name(), encoding_desc->name(),
                    stats.compressed_bytes_read_request, column_meta.total_mem_footprint(), column_meta.num_rows(),
                    stats.io_count_request);
            fmt::print("{}\n", column_meta.DebugString());

        } else {
            // sub columns
            for (size_t sub_id = 0; sub_id < tablet_column.subcolumn_count(); sub_id++) {
                auto& sub_column = tablet_column.subcolumn(sub_id);
                std::string sub_column_name = std::string(sub_column.name());

                // reset the stats
                OlapReaderStatistics stats;
                seg_opts.stats = &stats;

                // access path
                std::vector<ColumnAccessPathPtr> access_paths;
                seg_opts.column_access_paths = &access_paths;
                auto maybe_path = ColumnAccessPath::create(TAccessPathType::FIELD, "", id);
                RETURN_IF_ERROR(maybe_path);
                ColumnAccessPath::insert_json_path(maybe_path.value().get(), sub_column.type(), sub_column_name);
                access_paths.emplace_back(std::move(maybe_path.value()));

                // read it
                read_the_segment().ok();

                const ColumnMetaPB& sub_column_meta = column_meta.children_columns(sub_id);

                fmt::print(">>>>>>>>>>>>> sub column start >>>>>>>>>>>>>>>\n");
                fmt::print("[ column id: {} subcolumn {} {} compressed bytes: {} pages: {}]\n", id, sub_id,
                           sub_column_name, stats.compressed_bytes_read_request, stats.io_count_request);
                std::string meta_string = sub_column_meta.DebugString();
                fmt::print("{}\n", meta_string);
                fmt::print(">>>>>>>>>>>>> sub column end >>>>>>>>>>>>>>>\n");
            }
        }
    }

    return Status::OK();
}

} // namespace starrocks

int meta_tool_main(int argc, char** argv) {
    bool empty_args = (argc <= 1);
    std::string usage = get_usage(argv[0]);
    gflags::SetUsageMessage(usage);
    google::ParseCommandLineFlags(&argc, &argv, true);
    starrocks::date::init_date_cache();
    starrocks::config::disable_storage_page_cache = true;
    starrocks::MemChunkAllocator::init_metrics();

    if (empty_args || FLAGS_operation.empty()) {
        show_usage();
        return -1;
    }

    if (FLAGS_operation == "show_meta") {
        show_meta();
    } else if (FLAGS_operation == "batch_delete_meta") {
        std::string tablet_file;
        Status st = starrocks::fs::canonicalize(FLAGS_tablet_file, &tablet_file);
        if (!st.ok()) {
            std::cout << "invalid tablet file: " << FLAGS_tablet_file << ", error: " << st.to_string() << std::endl;
            return -1;
        }

        batch_delete_meta(tablet_file);
    } else if (FLAGS_operation == "show_segment_footer") {
        if (FLAGS_file == "") {
            std::cout << "no file flag for show dict" << std::endl;
            return -1;
        }
        show_segment_footer(FLAGS_file);
    } else if (FLAGS_operation == "dump_segment_data") {
        if (FLAGS_file == "") {
            std::cout << "no file flag for dump segment file" << std::endl;
            return -1;
        }
        starrocks::SegmentDump segment_dump(FLAGS_file);
        Status st = segment_dump.dump_segment_data();
        if (!st.ok()) {
            std::cout << "dump segment data failed: " << st << std::endl;
            return -1;
        }
    } else if (FLAGS_operation == "dump_column_size") {
        if (FLAGS_file == "") {
            std::cout << "no file flag for dump segment file" << std::endl;
            return -1;
        }
        starrocks::SegmentDump segment_dump(FLAGS_file);
        Status st = segment_dump.dump_column_size();
        if (!st.ok()) {
            std::cout << "dump column size failed: " << st << std::endl;
            return -1;
        }
    } else if (FLAGS_operation == "print_pk_dump") {
        if (FLAGS_file == "") {
            std::cout << "no file flag for pk dump file" << std::endl;
            return -1;
        }
        starrocks::PrimaryKeyDumpPB dump_pb;
        Status st = starrocks::PrimaryKeyDump::read_deserialize_from_file(FLAGS_file, &dump_pb);
        if (!st.ok()) {
            std::cout << "print pk dump failed: " << st << std::endl;
            return -1;
        }
        std::cout << "[pk dump] meta: " << dump_pb.Utf8DebugString() << std::endl;
        st = starrocks::PrimaryKeyDump::deserialize_pkcol_pkindex_from_meta(
                FLAGS_file, dump_pb,
                [&](const starrocks::Chunk& chunk) {
                    for (int i = 0; i < chunk.num_rows(); i++) {
                        std::cout << "pk column " << chunk.debug_row(i) << std::endl;
                    }
                },
                [&](const std::string& filename, const starrocks::PartialKVsPB& kvs) {
                    std::cout << " pk index, filename: " << filename << std::endl;
                    for (int i = 0; i < kvs.keys_size(); i++) {
                        std::cout << "index key " << starrocks::hexdump(kvs.keys(i).data(), kvs.keys(i).size())
                                  << " value " << kvs.values(i) << std::endl;
                    }
                });
        if (!st.ok()) {
            std::cout << "print pk dump failed: " << st << std::endl;
            return -1;
        }

    } else if (FLAGS_operation == "dump_short_key_index") {
        if (FLAGS_file == "") {
            std::cout << "no file set for dump short key index" << std::endl;
            return -1;
        }
        if (FLAGS_key_column_count == 0) {
            std::cout << "no key_column_count for dump short key index" << std::endl;
            return -1;
        }
        starrocks::SegmentDump segment_dump(FLAGS_file);
        Status st = segment_dump.dump_short_key_index(FLAGS_key_column_count);
        if (!st.ok()) {
            std::cout << "dump short key index failed: " << st << std::endl;
            return -1;
        }
    } else if (FLAGS_operation == "calc_checksum") {
        if (FLAGS_file == "") {
            std::cout << "no file flag for calc checksum" << std::endl;
            return -1;
        }
        starrocks::SegmentDump segment_dump(FLAGS_file, FLAGS_column_index);
        Status st = segment_dump.calc_checksum();
        if (!st.ok()) {
            std::cout << "dump segment data failed: " << st.message() << std::endl;
            return -1;
        }
    } else if (FLAGS_operation == "print_lake_metadata") {
        starrocks::TabletMetadataPB metadata;
        if (!metadata.ParseFromIstream(&std::cin)) {
            std::cerr << "Fail to parse tablet metadata\n";
            return -1;
        }
        json2pb::Pb2JsonOptions options;
        options.pretty_json = true;
        std::string json;
        std::string error;
        if (!json2pb::ProtoMessageToJson(metadata, &json, options, &error)) {
            std::cerr << "Fail to convert protobuf to json: " << error << '\n';
            return -1;
        }
        std::cout << json << '\n';
    } else if (FLAGS_operation == "print_lake_bundle_metadata") {
        std::string input_data((std::istreambuf_iterator<char>(std::cin)), std::istreambuf_iterator<char>());
        auto file_size = input_data.size();
        auto bundle_metadata_or = starrocks::lake::TabletManager::parse_bundle_tablet_metadata("input", input_data);
        if (!bundle_metadata_or.ok()) {
            std::cerr << "Fail to parse bundle metadata: " << bundle_metadata_or.status() << '\n';
            return -1;
        }
        const auto& bundle_metadata = bundle_metadata_or.value();
        // print bundle metadata proto as string
        std::cout << "Bundle Metadata: " << bundle_metadata->Utf8DebugString() << '\n';
        // foreach tablet_meta from bundle_metadata.tablet_meta_pages
        for (const auto& page : bundle_metadata->tablet_meta_pages()) {
            const starrocks::PagePointerPB& page_pointer = page.second;
            auto offset = page_pointer.offset();
            auto size = page_pointer.size();
            if (offset + size > file_size) {
                std::cerr << "Invalid page pointer for tablet " << page.first << ": offset + size exceeds file size\n";
                return -1;
            }

            auto metadata = std::make_shared<starrocks::TabletMetadataPB>();
            std::string_view metadata_str = std::string_view(input_data.data() + offset);
            if (!metadata->ParseFromArray(metadata_str.data(), size)) {
                std::cerr << "Fail to parse tablet metadata for tablet " << page.first << '\n';
                return -1;
            }

            int64_t tablet_id = page.first;
            std::cout << "Tablet ID: " << tablet_id << '\n';
            auto schema_id = bundle_metadata->tablet_to_schema().find(tablet_id);
            if (schema_id == bundle_metadata->tablet_to_schema().end()) {
                std::cerr << "tablet " << tablet_id
                          << " metadata can not find schema in shared metadata, maybe the bundle is not complete\n";
                return -1;
            }
            auto schema_it = bundle_metadata->schemas().find(schema_id->second);
            if (schema_it == bundle_metadata->schemas().end()) {
                std::cerr << "tablet " << tablet_id << " metadata can not find schema(" << schema_id->second
                          << ") in shared metadata, maybe the bundle is not complete\n";
                return -1;
            } else {
                metadata->mutable_schema()->CopyFrom(schema_it->second);
                auto& item = (*metadata->mutable_historical_schemas())[schema_id->second];
                item.CopyFrom(schema_it->second);
            }

            for (auto& [_, schema_id] : metadata->rowset_to_schema()) {
                schema_it = bundle_metadata->schemas().find(schema_id);
                if (schema_it == bundle_metadata->schemas().end()) {
                    std::cerr << "rowset metadata can not find schema(" << schema_id
                              << ") in shared metadata, maybe the bundle is not complete\n";
                    return -1;
                } else {
                    auto& item = (*metadata->mutable_historical_schemas())[schema_id];
                    item.CopyFrom(schema_it->second);
                }
            }
            json2pb::Pb2JsonOptions options;
            options.pretty_json = true;
            std::string json;
            std::string error;
            if (!json2pb::ProtoMessageToJson(*metadata, &json, options, &error)) {
                std::cerr << "Fail to convert protobuf to json: " << error << '\n';
                return -1;
            }
            std::cout << json << '\n';
        }
    } else if (FLAGS_operation == "print_lake_txn_log") {
        starrocks::TxnLogPB txn_log;
        if (!txn_log.ParseFromIstream(&std::cin)) {
            std::cerr << "Fail to parse txn log\n";
            return -1;
        }
        json2pb::Pb2JsonOptions options;
        options.pretty_json = true;
        std::string json;
        std::string error;
        if (!json2pb::ProtoMessageToJson(txn_log, &json, options, &error)) {
            std::cerr << "Fail to convert protobuf to json: " << error << '\n';
            return -1;
        }
        std::cout << json << '\n';
    } else if (FLAGS_operation == "print_lake_schema") {
        starrocks::TabletSchemaPB schema;
        if (!schema.ParseFromIstream(&std::cin)) {
            std::cerr << "Fail to parse schema\n";
            return -1;
        }
        json2pb::Pb2JsonOptions options;
        options.pretty_json = true;
        std::string json;
        std::string error;
        if (!json2pb::ProtoMessageToJson(schema, &json, options, &error)) {
            std::cerr << "Fail to convert protobuf to json: " << error << '\n';
            return -1;
        }
        std::cout << json << '\n';
    } else if (FLAGS_operation == "lake_datafile_gc") {
        if (!starrocks::config::init(FLAGS_conf_file.c_str())) {
            std::cerr << "Init config failed, conf file: " << FLAGS_conf_file << std::endl;
            return -1;
        }
        if (!starrocks::init_glog("lake_datafile_gc", true)) {
            std::cerr << "Init glog failed" << std::endl;
            return -1;
        }
        if (FLAGS_expired_sec < 600) {
            std::cerr << "expired_sec is less than 10min" << std::endl;
            return -1;
        }
        Aws::SDKOptions options;
        Aws::InitAPI(options);
        auto status =
                starrocks::lake::datafile_gc(FLAGS_root_path, FLAGS_audit_file, FLAGS_expired_sec, FLAGS_do_delete);
        if (!status.ok()) {
            std::cout << status << std::endl;
        }
        starrocks::close_s3_clients();
        Aws::ShutdownAPI(options);
    } else {
        // operations that need root path should be written here
        std::set<std::string> valid_operations = {"get_meta",
                                                  "load_meta",
                                                  "delete_meta",
                                                  "delete_rowset_meta",
                                                  "get_persistent_index_meta",
                                                  "delete_persistent_index_meta",
                                                  "compact_meta",
                                                  "get_meta_stats",
                                                  "ls",
                                                  "check_table_meta_consistency",
                                                  "scan_dcgs"};
        if (valid_operations.find(FLAGS_operation) == valid_operations.end()) {
            std::cout << "invalid operation: " << FLAGS_operation << std::endl << std::endl;
            show_usage();
            return -1;
        }

        bool read_only = false;
        if (FLAGS_operation == "get_meta" || FLAGS_operation == "get_meta_stats" || FLAGS_operation == "ls" ||
            FLAGS_operation == "check_table_meta_consistency" || FLAGS_operation == "scan_dcgs" ||
            FLAGS_operation == "get_persistent_index_meta") {
            read_only = true;
        }

        if (FLAGS_root_path.empty()) {
            std::cout << "--root_path option is required for operation: " << FLAGS_operation << "!" << std::endl;
            return -1;
        }
        std::unique_ptr<DataDir> data_dir;
        Status st = init_data_dir(FLAGS_root_path, &data_dir, read_only);
        if (!st.ok()) {
            std::cout << "invalid root path:" << FLAGS_root_path << ", error: " << st.to_string() << std::endl;
            return -1;
        }

        if (FLAGS_operation == "get_meta") {
            get_meta(data_dir.get());
        } else if (FLAGS_operation == "load_meta") {
            load_meta(data_dir.get());
        } else if (FLAGS_operation == "delete_meta") {
            delete_meta(data_dir.get());
        } else if (FLAGS_operation == "delete_rowset_meta") {
            delete_rowset_meta(data_dir.get());
        } else if (FLAGS_operation == "get_persistent_index_meta") {
            get_persistent_index_meta(data_dir.get());
        } else if (FLAGS_operation == "delete_persistent_index_meta") {
            delete_persistent_index_meta(data_dir.get());
        } else if (FLAGS_operation == "compact_meta") {
            compact_meta(data_dir.get());
        } else if (FLAGS_operation == "get_meta_stats") {
            get_meta_stats(data_dir.get());
        } else if (FLAGS_operation == "ls") {
            list_meta(data_dir.get());
        } else if (FLAGS_operation == "check_table_meta_consistency") {
            check_meta_consistency(data_dir.get());
        } else if (FLAGS_operation == "scan_dcgs") {
            scan_dcgs(data_dir.get());
        } else {
            std::cout << "invalid operation: " << FLAGS_operation << std::endl << std::endl;
            show_usage();
            return -1;
        }
    }
    gflags::ShutDownCommandLineFlags();
    return 0;
}
