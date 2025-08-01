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

package com.starrocks.catalog;

import com.starrocks.lake.LakeTable;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.DropDbStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.commons.collections.map.HashedMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ColocateTableIndexTest {
    private static final Logger LOG = LogManager.getLogger(ColocateTableIndexTest.class);

    @BeforeEach
    public void setUp() {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.setUpForPersistTest();
    }

    @AfterEach
    public void teardown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    /**
     * [
     * [10002.10006, 10002_group1, 10004, 10016, 4, 1, int(11), true],
     * [10026.10030, 10026_group2, 10028, 4, 1, int(11), true]
     * ]
     * ->
     * {
     * 'group1': [10002.10006, 10002_group1, 10004, 10016, 4, 1, int(11), true],
     * 'group2': [10026.10030, 10026_group2, 10028, 4, 1, int(11), true]
     * }
     */
    private Map<String, List<String>> groupByName(List<List<String>> lists) {
        Map<String, List<String>> ret = new HashedMap();
        for (List<String> list : lists) {
            ret.put(list.get(1).split("_")[1], list);
        }
        return ret;
    }

    @Test
    public void testDropTable() throws Exception {
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();

        // create db1
        String createDbStmtStr = "create database db1;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());

        // create table1_1->group1
        String sql = "CREATE TABLE db1.table1_1 (k1 int, k2 int, k3 varchar(32))\n" +
                "PRIMARY KEY(k1)\n" +
                "DISTRIBUTED BY HASH(k1)\n" +
                "BUCKETS 4\n" +
                "PROPERTIES(\"colocate_with\"=\"group1\", \"replication_num\" = \"1\");\n";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
        List<List<String>> infos = GlobalStateMgr.getCurrentState().getColocateTableIndex().getInfos();
        // group1->table1To1
        Assertions.assertEquals(1, infos.size());
        Map<String, List<String>> map = groupByName(infos);
        Table table1To1 = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db1").getTable("table1_1");
        Assertions.assertEquals(String.format("%d", table1To1.getId()), map.get("group1").get(2));
        LOG.info("after create db1.table1_1: {}", infos);

        // create table1_2->group1
        sql = "CREATE TABLE db1.table1_2 (k1 int, k2 int, k3 varchar(32))\n" +
                "PRIMARY KEY(k1)\n" +
                "DISTRIBUTED BY HASH(k1)\n" +
                "BUCKETS 4\n" +
                "PROPERTIES(\"colocate_with\"=\"group1\", \"replication_num\" = \"1\");\n";
        createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
        // group1 -> table1To1, table1To2
        infos = GlobalStateMgr.getCurrentState().getColocateTableIndex().getInfos();
        Assertions.assertEquals(1, infos.size());
        map = groupByName(infos);
        Table table1To2 = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db1").getTable("table1_2");
        Assertions.assertEquals(String.format("%d, %d", table1To1.getId(), table1To2.getId()), map.get("group1").get(2));
        LOG.info("after create db1.table1_2: {}", infos);

        // create db2
        createDbStmtStr = "create database db2;";
        createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        // create table2_1 -> group2
        sql = "CREATE TABLE db2.table2_1 (k1 int, k2 int, k3 varchar(32))\n" +
                "PRIMARY KEY(k1)\n" +
                "DISTRIBUTED BY HASH(k1)\n" +
                "BUCKETS 4\n" +
                "PROPERTIES(\"colocate_with\"=\"group2\", \"replication_num\" = \"1\");\n";
        createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
        // group1 -> table1_1, table1_2
        // group2 -> table2_l
        infos = GlobalStateMgr.getCurrentState().getColocateTableIndex().getInfos();
        Assertions.assertEquals(2, infos.size());
        map = groupByName(infos);
        Assertions.assertEquals(String.format("%d, %d", table1To1.getId(), table1To2.getId()), map.get("group1").get(2));
        Table table2To1 = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db2").getTable("table2_1");
        Assertions.assertEquals(String.format("%d", table2To1.getId()), map.get("group2").get(2));
        LOG.info("after create db2.table2_1: {}", infos);

        // drop db1.table1_1
        sql = "DROP TABLE db1.table1_1;";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
        // group1 -> table1_1*, table1_2
        // group2 -> table2_l
        infos = GlobalStateMgr.getCurrentState().getColocateTableIndex().getInfos();
        map = groupByName(infos);
        Assertions.assertEquals(2, infos.size());
        Assertions.assertEquals(String.format("%d*, %d", table1To1.getId(), table1To2.getId()), map.get("group1").get(2));
        Assertions.assertEquals(String.format("%d", table2To1.getId()), map.get("group2").get(2));
        LOG.info("after drop db1.table1_1: {}", infos);

        // drop db1.table1_2
        sql = "DROP TABLE db1.table1_2;";
        dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
        // group1 -> table1_1*, table1_2*
        // group2 -> table2_l
        infos = GlobalStateMgr.getCurrentState().getColocateTableIndex().getInfos();
        map = groupByName(infos);
        Assertions.assertEquals(2, infos.size());
        Assertions.assertEquals(String.format("%d*, %d*", table1To1.getId(), table1To2.getId()), map.get("group1").get(2));
        Assertions.assertEquals(String.format("[deleted], [deleted]", table1To1.getId(), table1To2.getId()),
                map.get("group1").get(3));

        Assertions.assertEquals(String.format("%d", table2To1.getId()), map.get("group2").get(2));
        Assertions.assertEquals(String.format("table2_1", table2To1.getId()), map.get("group2").get(3));

        LOG.info("after drop db1.table1_2: {}", infos);

        // drop db2
        sql = "DROP DATABASE db2;";
        DropDbStmt dropDbStmt = (DropDbStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore()
                .dropDb(connectContext, dropDbStmt.getDbName(), dropDbStmt.isForceDrop());
        // group1 -> table1_1*, table1_2*
        // group2 -> table2_l*
        infos = GlobalStateMgr.getCurrentState().getColocateTableIndex().getInfos();
        map = groupByName(infos);
        Assertions.assertEquals(2, infos.size());
        Assertions.assertEquals(String.format("%d*, %d*", table1To1.getId(), table1To2.getId()), map.get("group1").get(2));
        Assertions.assertEquals(String.format("%d*", table2To1.getId()), map.get("group2").get(2));
        LOG.info("after drop db2: {}", infos);

        // create & drop db2 again
        createDbStmtStr = "create database db2;";
        createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        // create table2_1 -> group2
        sql = "CREATE TABLE db2.table2_3 (k1 int, k2 int, k3 varchar(32))\n" +
                "PRIMARY KEY(k1)\n" +
                "DISTRIBUTED BY HASH(k1)\n" +
                "BUCKETS 4\n" +
                "PROPERTIES(\"colocate_with\"=\"group3\", \"replication_num\" = \"1\");\n";
        createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
        Table table2To3 = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db2").getTable("table2_3");
        sql = "DROP DATABASE db2;";
        dropDbStmt = (DropDbStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore()
                .dropDb(connectContext, dropDbStmt.getDbName(), dropDbStmt.isForceDrop());
        infos = GlobalStateMgr.getCurrentState().getColocateTableIndex().getInfos();
        map = groupByName(infos);
        LOG.info("after create & drop db2: {}", infos);
        Assertions.assertEquals(3, infos.size());
        Assertions.assertEquals(String.format("%d*, %d*", table1To1.getId(), table1To2.getId()), map.get("group1").get(2));
        Assertions.assertEquals("[deleted], [deleted]", map.get("group1").get(3));
        Assertions.assertEquals(String.format("%d*", table2To1.getId()), map.get("group2").get(2));
        Assertions.assertEquals(String.format("[deleted], [deleted]", table1To1.getId(), table1To2.getId()),
                map.get("group1").get(3));
        Assertions.assertEquals(String.format("%d*", table2To3.getId()), map.get("group3").get(2));
        Assertions.assertEquals(String.format("[deleted], [deleted]", table1To1.getId(), table1To2.getId()),
                map.get("group1").get(3));
    }

    @Test
    public void testCleanUp() throws Exception {
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();

        // create goodDb
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser("create database goodDb;", connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        Database goodDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("goodDb");
        // create goodtable
        String sql = "CREATE TABLE " +
                "goodDb.goodTable (k1 int, k2 int, k3 varchar(32))\n" +
                "PRIMARY KEY(k1)\n" +
                "DISTRIBUTED BY HASH(k1)\n" +
                "BUCKETS 4\n" +
                "PROPERTIES(\"colocate_with\"=\"goodGroup\", \"replication_num\" = \"1\");\n";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
        OlapTable table =
                (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(goodDb.getFullName(), "goodTable");
        ColocateTableIndex.GroupId goodGroup = GlobalStateMgr.getCurrentState().getColocateTableIndex().getGroup(table.getId());

        // create a bad db
        long badDbId = 4000;
        table.id = 4001;
        table.name = "goodTableOfBadDb";
        colocateTableIndex.addTableToGroup(
                badDbId, table, "badGroupOfBadDb", new ColocateTableIndex.GroupId(badDbId, 4002), false);
        // create a bad table in good db
        table.id = 4003;
        table.name = "badTable";
        colocateTableIndex.addTableToGroup(
                goodDb.getId(), table, "badGroupOfBadTable", new ColocateTableIndex.GroupId(goodDb.getId(), 4004), false);

        Map<String, List<String>> map = groupByName(GlobalStateMgr.getCurrentState().getColocateTableIndex().getInfos());
        Assertions.assertTrue(map.containsKey("goodGroup"));
        Assertions.assertTrue(map.containsKey("badGroupOfBadDb"));
        Assertions.assertTrue(map.containsKey("badGroupOfBadTable"));

        colocateTableIndex.cleanupInvalidDbOrTable(GlobalStateMgr.getCurrentState());
        map = groupByName(GlobalStateMgr.getCurrentState().getColocateTableIndex().getInfos());

        Assertions.assertTrue(map.containsKey("goodGroup"));
        Assertions.assertFalse(map.containsKey("badGroupOfBadDb"));
        Assertions.assertFalse(map.containsKey("badGroupOfBadTable"));
    }

    @Test
    public void testLakeTableColocation(@Mocked LakeTable olapTable, @Mocked StarOSAgent starOSAgent) throws Exception {
        ColocateTableIndex colocateTableIndex = new ColocateTableIndex();

        new MockUp<GlobalStateMgr>() {
            @Mock
            public StarOSAgent getStarOSAgent() {
                return starOSAgent;
            }
        };

        long dbId = 100;
        long dbId2 = 101;
        long tableId = 200;
        new MockUp<OlapTable>() {
            @Mock
            public long getId() {
                return tableId;
            }

            @Mock
            public boolean isCloudNativeTable() {
                return true;
            }

            @Mock
            public DistributionInfo getDefaultDistributionInfo() {
                return new HashDistributionInfo();
            }
        };

        new MockUp<LakeTable>() {
            @Mock
            public List<Long> getShardGroupIds() {
                return new ArrayList<>();
            }
        };

        colocateTableIndex.addTableToGroup(
                dbId, (OlapTable) olapTable, "lakeGroup", new ColocateTableIndex.GroupId(dbId, 10000), false /* isReplay */);
        Assertions.assertTrue(colocateTableIndex.isLakeColocateTable(tableId));
        colocateTableIndex.addTableToGroup(
                dbId2, (OlapTable) olapTable, "lakeGroup", new ColocateTableIndex.GroupId(dbId2, 10000),
                false /* isReplay */);
        Assertions.assertTrue(colocateTableIndex.isLakeColocateTable(tableId));

        Assertions.assertTrue(colocateTableIndex.isGroupUnstable(new ColocateTableIndex.GroupId(dbId, 10000)));
        Assertions.assertFalse(colocateTableIndex.isGroupUnstable(new ColocateTableIndex.GroupId(dbId, 10001)));

        colocateTableIndex.removeTable(tableId, null, false /* isReplay */);
        Assertions.assertFalse(colocateTableIndex.isLakeColocateTable(tableId));
    }

    @Test
    public void testSaveLoadJsonFormatImage() throws Exception {
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();

        // create goodDb
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils
                .parseStmtWithNewParser("create database db_image;", connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db_image");
        // create goodtable
        String sql = "CREATE TABLE " +
                "db_image.tbl1 (k1 int, k2 int, k3 varchar(32))\n" +
                "PRIMARY KEY(k1)\n" +
                "DISTRIBUTED BY HASH(k1)\n" +
                "BUCKETS 4\n" +
                "PROPERTIES(\"colocate_with\"=\"goodGroup\", \"replication_num\" = \"1\");\n";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "tbl1");

        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        colocateTableIndex.saveColocateTableIndexV2(image.getImageWriter());

        ColocateTableIndex followerIndex = new ColocateTableIndex();
        SRMetaBlockReader reader = new SRMetaBlockReaderV2(image.getJsonReader());
        followerIndex.loadColocateTableIndexV2(reader);
        reader.close();
        Assertions.assertEquals(colocateTableIndex.getAllGroupIds(), followerIndex.getAllGroupIds());
        Assertions.assertEquals(colocateTableIndex.getGroup(table.getId()), followerIndex.getGroup(table.getId()));

        UtFrameUtils.tearDownForPersisTest();
    }
}
