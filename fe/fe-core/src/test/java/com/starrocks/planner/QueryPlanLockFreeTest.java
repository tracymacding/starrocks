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

package com.starrocks.planner;

import com.starrocks.catalog.OlapTable;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class QueryPlanLockFreeTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static String DB_NAME = "test";

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();

        FeConstants.enablePruneEmptyOutputScan = false;

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.setQueryId(UUIDUtil.genUUID());
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);

        starRocksAssert.withTable("CREATE TABLE IF NOT EXISTS `t0` (\n" +
                "  `k1` int(11) NULL,\n" +
                "  `k2` int(11) NULL,\n" +
                "  `k3` int(11) NULL,\n" +
                "  `v1` int SUM NULL,\n" +
                "  `v2` bigint SUM NULL,\n" +
                "  `v3` largeint SUM NULL,\n" +
                "  `v4` double SUM NULL,\n" +
                "  `v5` decimal(10, 3) SUM NULL\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(`k1`, `k2`, `k3`)\n" +
                "DISTRIBUTED BY HASH(`k2`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE IF NOT EXISTS `t1` (\n" +
                "  `k1` int(11) NULL,\n" +
                "  `k2` int(11) NULL,\n" +
                "  `k3` int(11) NULL,\n" +
                "  `v1` int SUM NULL,\n" +
                "  `v2` bigint SUM NULL,\n" +
                "  `v3` largeint SUM NULL,\n" +
                "  `v4` double SUM NULL,\n" +
                "  `v5` decimal(10, 3) SUM NULL\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(`k1`, `k2`, `k3`)\n" +
                "DISTRIBUTED BY HASH(`k2`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\"\n" +
                ");");
    }

    @Test
    public void testPlanStrategy() throws Exception {
        String sql = "select * from t0";
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getTable(new ConnectContext(), "default_catalog", DB_NAME, "t0");
        table.lastSchemaUpdateTime.set(System.nanoTime() + 10000000000L);
        Assertions.assertThrows(StarRocksPlannerException.class, () -> UtFrameUtils.getPlanAndFragment(connectContext, sql), "schema of [t0] had been updated frequently during the plan generation");

        connectContext.getSessionVariable().setCboUseDBLock(true);
        Pair<String, ExecPlan> plan = UtFrameUtils.getPlanAndFragment(connectContext, sql);
        Assertions.assertTrue(plan.first.contains("SCAN"), plan.first);
        connectContext.getSessionVariable().setCboUseDBLock(false);

        // follower node
        GlobalStateMgr.getCurrentState().setFrontendNodeType(FrontendNodeType.FOLLOWER);
        Assertions.assertThrows(StarRocksPlannerException.class, () -> UtFrameUtils.getPlanAndFragment(connectContext, sql), "schema of [t0] had been updated frequently during the plan generation");
    }

    @Test
    public void testCopiedTable() throws Exception {
        String sql = "select t1.* from t1 t1 join t1 t2 on t1.k1 = t2.k2";
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getTable(new ConnectContext(), "default_catalog", DB_NAME, "t1");
        Pair<String, ExecPlan> plan = UtFrameUtils.getPlanAndFragment(connectContext, sql);
        OlapScanNode node1 = (OlapScanNode) plan.second.getScanNodes().get(0);
        OlapScanNode node2 = (OlapScanNode) plan.second.getScanNodes().get(1);
        Assertions.assertTrue(table != node1.getOlapTable(), "original table should different from copied table in plan");
        Assertions.assertTrue(table != node2.getOlapTable(), "original table should different from copied table in plan");

        Assertions.assertTrue(node2.getOlapTable() == node1.getOlapTable(), "copied tables should share the same object");
    }

}
