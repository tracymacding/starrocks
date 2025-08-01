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

package com.starrocks.scheduler;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Partition;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.MethodName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import static com.starrocks.sql.plan.ConnectorPlanTestBase.MOCK_PAIMON_CATALOG_NAME;

@TestMethodOrder(MethodName.class)
public class PartitionBasedMvRefreshProcessorPaimonTest extends MVTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        ConnectorPlanTestBase.mockCatalog(connectContext, MOCK_PAIMON_CATALOG_NAME, newFolder(temp, "junit").toURI().toString());
    }

    @Test
    public void testcreateUnpartitionedPmnMaterializeView() {
        //unparitioned
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `test`.`paimon_parttbl_mv2`\n" +
                        "COMMENT \"MATERIALIZED_VIEW\"\n" +
                        "DISTRIBUTED BY HASH(`pk`) BUCKETS 10\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"" +
                        ")\n" +
                        "AS SELECT pk, d  FROM `paimon0`.`pmn_db1`.`unpartitioned_table` as a;",
                () -> {
                    Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

                    MaterializedView unpartitionedMaterializedView =
                            ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                                    .getTable(testDb.getFullName(), "paimon_parttbl_mv2"));
                    triggerRefreshMv(testDb, unpartitionedMaterializedView);

                    Collection<Partition> partitions = unpartitionedMaterializedView.getPartitions();
                    Assertions.assertEquals(1, partitions.size());

                    String query = "SELECT pk, d  FROM `paimon0`.`pmn_db1`.`unpartitioned_table` as a";
                    String plan = getFragmentPlan(query);
                    PlanTestBase.assertContains(plan, "     TABLE: paimon_parttbl_mv2\n" +
                            "     PREAGGREGATION: ON\n" +
                            "     partitions=1/1");
                });
    }

    @Test
    public void testCreatePartitionedPmnMaterializeView() {
        //paritioned
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `test`.`paimon_parttbl_mv1`\n" +
                        "COMMENT \"MATERIALIZED_VIEW\"\n" +
                        "PARTITION BY (`pt`)\n" +
                        "DISTRIBUTED BY HASH(`pk`) BUCKETS 10\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ")\n" +
                        "AS SELECT pk, pt,d  FROM `paimon0`.`pmn_db1`.`partitioned_table` as a;",
                () -> {
                    Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
                    MaterializedView partitionedMaterializedView =
                            ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                                    .getTable(testDb.getFullName(), "paimon_parttbl_mv1"));
                    triggerRefreshMv(testDb, partitionedMaterializedView);

                    Collection<Partition> partitions = partitionedMaterializedView.getPartitions();
                    Assertions.assertEquals(10, partitions.size());
                    triggerRefreshMv(testDb, partitionedMaterializedView);

                    Assertions.assertEquals(10, partitionedMaterializedView.getPartitions().size());
                    triggerRefreshMv(testDb, partitionedMaterializedView);

                    String query = "SELECT d, count(pk) FROM " +
                            "`paimon0`.`pmn_db1`.`partitioned_table` as a group by d;";
                    String plan = getFragmentPlan(query);
                    System.out.println(plan);
                    PlanTestBase.assertContains(plan, "     TABLE: paimon_parttbl_mv1\n" +
                            "     PREAGGREGATION: ON\n" +
                            "     partitions=10/10\n" +
                            "     rollup: paimon_parttbl_mv1");
                });
    }

    private static void triggerRefreshMv(Database testDb, MaterializedView mv)
            throws Exception {
        Task task = TaskBuilder.buildMvTask(mv, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        initAndExecuteTaskRun(taskRun);
    }

    private static File newFolder(File root, String... subDirs) throws IOException {
        String subFolder = String.join("/", subDirs);
        File result = new File(root, subFolder);
        if (!result.mkdirs()) {
            throw new IOException("Couldn't create folders " + root);
        }
        return result;
    }
}