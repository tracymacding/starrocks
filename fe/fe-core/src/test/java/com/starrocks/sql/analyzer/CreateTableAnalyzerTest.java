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

package com.starrocks.sql.analyzer;

import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CreateTableAnalyzerTest {

    private static ConnectContext connectContext;


    @BeforeAll
    public static void beforeClass() throws Exception {

        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test_create_table_db");
    }

    @AfterAll
    public static void afterClass() throws Exception {
        Config.max_column_number_per_table = 10000;
    }

    @Test
    public void testAnalyze() {
        String sql = "CREATE TABLE test_create_table_db.starrocks_test_table\n" +
                "(\n" +
                "    `tag_id` string,\n" +
                "    `tag_name` string\n" +
                ") ENGINE = OLAP PRIMARY KEY(`id`)\n" +
                "DISTRIBUTED BY HASH(`id`)\n" +
                "ORDER BY(`id`)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"enable_persistent_index\" = \"true\",\n" +
                "\"replicated_storage\" = \"true\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ")\n";

        Throwable exception = assertThrows(SemanticException.class, () -> {
            CreateTableStmt createTableStmt = (CreateTableStmt) com.starrocks.sql.parser.SqlParser
                    .parse(sql, connectContext.getSessionVariable().getSqlMode()).get(0);
            CreateTableAnalyzer.analyze(createTableStmt, connectContext);
        });
        assertThat(exception.getMessage(), containsString("doesn't exist"));
    }

    @Test
    public void testAnalyzeMaxBucket() {
        Config.max_column_number_per_table = 10000;

        String sql = "CREATE TABLE test_create_table_db.starrocks_test_table\n" +
                "(\n" +
                "    `tag_id` bigint not null,\n" +
                "    `tag_name` string\n" +
                ") DUPLICATE KEY(`tag_id`)\n" +
                "PARTITION BY (`tag_id`)\n" +
                "DISTRIBUTED BY HASH(`tag_id`) BUCKETS 1025\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n";

        Throwable exception = assertThrows(SemanticException.class, () -> {
            CreateTableStmt createTableStmt = (CreateTableStmt) com.starrocks.sql.parser.SqlParser
                    .parse(sql, connectContext.getSessionVariable().getSqlMode()).get(0);
            CreateTableAnalyzer.analyze(createTableStmt, connectContext);
        });
        assertThat(exception.getMessage(), containsString("max_bucket_number_per_partition"));
    }

    @Test
    public void testMaxColumn() {
        Config.max_column_number_per_table = 1;

        String sql = "CREATE TABLE test_create_table_db.starrocks_test_table\n" +
                "(\n" +
                "    `tag_id` bigint not null,\n" +
                "    `tag_name` string\n" +
                ") DUPLICATE KEY(`tag_id`)\n" +
                "PARTITION BY (`tag_id`)\n" +
                "DISTRIBUTED BY HASH(`tag_id`)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n";
        Throwable exception = assertThrows(SemanticException.class, () -> {
            CreateTableStmt createTableStmt = (CreateTableStmt) com.starrocks.sql.parser.SqlParser
                    .parse(sql, connectContext.getSessionVariable().getSqlMode()).get(0);
            CreateTableAnalyzer.analyze(createTableStmt, connectContext);
        });
        assertThat(exception.getMessage(), containsString("max_column_number_per_table"));
    }
}
