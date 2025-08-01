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
//   https://github.com/apache/incubator-doris/blob/master/be/test/runtime/user_function_cache_test.cpp

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

#include "runtime/user_function_cache.h"

#include <gtest/gtest.h>

#include <cstdio>
#include <cstdlib>

#include "common/config.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "fs/fs_util.h"
#include "http/ev_http_server.h"
#include "http/http_channel.h"
#include "http/http_handler.h"
#include "http/http_request.h"
#include "util/md5.h"

int main(int argc, char* argv[]);

namespace starrocks {

bool k_is_downloaded = false;
class UserFunctionTestHandler : public HttpHandler {
public:
    void handle(HttpRequest* req) override {
        auto& file_name = req->param("FILE");
        std::string lib_dir = "./be/test/runtime/test_data/user_function_cache/lib";
        auto lib_file = lib_dir + "/" + file_name;
        FILE* fp = fopen(lib_file.c_str(), "r");
        if (fp == nullptr) {
            HttpChannel::send_error(req, INTERNAL_SERVER_ERROR);
            return;
        }
        std::string response;
        char buf[1024];
        while (true) {
            auto size = fread(buf, 1, 1024, fp);
            response.append(buf, size);
            if (size < 1024) {
                break;
            }
        }
        HttpChannel::send_reply(req, response);
        k_is_downloaded = true;
        fclose(fp);
    }
};

static UserFunctionTestHandler s_test_handler = UserFunctionTestHandler();
static EvHttpServer* s_server = nullptr;
static int real_port = 0;
static std::string hostname = "";
static std::string my_add_md5sum;
static std::string jar_md5sum;
static std::string wasm_md5sum;

static std::string compute_md5(const std::string& file) {
    FILE* fp = fopen(file.c_str(), "r");
    Md5Digest md5;
    char buf[1024];
    while (true) {
        auto size = fread(buf, 1, 1024, fp);
        md5.update(buf, size);
        if (size < 1024) {
            break;
        }
    }
    fclose(fp);
    md5.digest();
    return md5.hex();
}
class UserFunctionCacheTest : public testing::Test {
public:
    UserFunctionCacheTest() = default;
    ~UserFunctionCacheTest() override = default;
    static void SetUpTestCase() {
        s_server = new EvHttpServer(0);
        s_server->register_handler(GET, "/{FILE}", &s_test_handler);
        s_server->start();
        real_port = s_server->get_real_port();
        ASSERT_NE(0, real_port);
        hostname = "http://127.0.0.1:" + std::to_string(real_port);

        int res = 0;

        // compile code to so
        // res =
        //         system("g++ -shared ./be/test/runtime/test_data/user_function_cache/lib/my_add.cc -o "
        //                "./be/test/runtime/test_data/user_function_cache/lib/my_add.so");

        // my_add_md5sum = compute_md5("./be/test/runtime/test_data/user_function_cache/lib/my_add.so");

        res = system("touch ./be/test/runtime/test_data/user_function_cache/lib/my_udf.jar");

        ASSERT_EQ(res, 0) << res;

        jar_md5sum = compute_md5("./be/test/runtime/test_data/user_function_cache/lib/my_udf.jar");

        res = system("touch ./be/test/runtime/test_data/user_function_cache/lib/my_udf.wasm");

        ASSERT_EQ(res, 0) << res;

        wasm_md5sum = compute_md5("./be/test/runtime/test_data/user_function_cache/lib/my_udf.wasm");
    }
    static void TearDownTestCase() {
        s_server->stop();
        s_server->join();
        delete s_server;
        int res = 0;
        // res = system("rm -rf ./be/test/runtime/test_data/user_function_cache/lib/my_add.so");
        res = system("rm -rf ./be/test/runtime/test_data/user_function_cache/lib/my_udf.jar");
        ASSERT_EQ(res, 0) << res;

        res = system("rm -rf ./be/test/runtime/test_data/user_function_cache/lib/my_udf.wasm");
        ASSERT_EQ(res, 0) << res;

        res = system("rm -rf ./be/test/runtime/test_data/user_function_cache/download/");

        ASSERT_EQ(res, 0) << res;
    }
    void SetUp() override { k_is_downloaded = false; }
};

TEST_F(UserFunctionCacheTest, test_function_type) {
    UserFunctionCache cache;
    std::string lib_dir = "./be/test/runtime/test_data/user_function_cache/normal";
    auto st = cache.init(lib_dir);
    ASSERT_TRUE(st.ok());

    {
        std::string URL = fmt::format("http://127.0.0.1:{}/test.jar", real_port);
        int tp = cache._get_function_type(URL);
        ASSERT_EQ(tp, UserFunctionCache::UDF_TYPE_JAVA);
    }
}

TEST_F(UserFunctionCacheTest, download_normal) {
    UserFunctionCache cache;
    std::string lib_dir = "./be/test/runtime/test_data/user_function_cache/download";
    fs::remove_all(lib_dir);
    auto st = cache.init(lib_dir);
    ASSERT_TRUE(st.ok()) << st;

    {
        std::string libpath;
        int fid = 0;
        std::string URL = fmt::format("http://127.0.0.1:{}/test.jar", real_port);
        (void)cache.get_libpath(fid, URL, jar_md5sum, TFunctionBinaryType::SRJAR, &libpath);
    }
}

TEST_F(UserFunctionCacheTest, clear_all_lib_file_before_start) {
    UserFunctionCache cache;
    std::string lib_dir = "./be/test/runtime/test_data/user_function_cache/clear";
    fs::remove_all(lib_dir);
    config::clear_udf_cache_when_start = true;
    auto st = cache.init(lib_dir);
    config::clear_udf_cache_when_start = false;
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_FALSE(fs::path_exist(lib_dir + "/1/1.1.jar"));
}

TEST_F(UserFunctionCacheTest, download_wasm) {
    UserFunctionCache cache;
    std::string lib_dir = "./be/test/runtime/test_data/user_function_cache/download";
    fs::remove_all(lib_dir);
    auto st = cache.init(lib_dir);
    ASSERT_TRUE(st.ok()) << st;

    {
        std::string libpath;
        int fid = 0;
        std::string URL = fmt::format("http://127.0.0.1:{}/test.wasm", real_port);
        (void)cache.get_libpath(fid, URL, wasm_md5sum, TFunctionBinaryType::SRJAR, &libpath);
    }
}

TEST_F(UserFunctionCacheTest, load_wasm) {
    UserFunctionCache cache;
    int res = 0;
    std::string lib_dir = "./be/test/runtime/test_data/user_function_cache/download";
    fs::remove_all(lib_dir);
    res = system("mkdir -p ./be/test/runtime/test_data/user_function_cache/download/0/");
    ASSERT_EQ(res, 0) << res;
    res = system("touch ./be/test/runtime/test_data/user_function_cache/download/0/test.wasm");
    ASSERT_EQ(res, 0) << res;
    auto st = cache.init(lib_dir);
    ASSERT_TRUE(st.ok()) << st;
}

} // namespace starrocks
