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

import com.google.common.collect.Lists;
import com.starrocks.analysis.TypeDef;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.ListPartitionDesc;
import com.starrocks.sql.ast.MultiItemListPartitionDesc;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.SingleItemListPartitionDesc;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TTabletType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class ListPartitionDescTest {


    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
    }

    private List<ColumnDef> findColumnDefList() {
        ColumnDef id = new ColumnDef("id", TypeDef.create(PrimitiveType.BIGINT));
        id.setAggregateType(AggregateType.NONE);
        ColumnDef userId = new ColumnDef("user_id", TypeDef.create(PrimitiveType.BIGINT));
        userId.setAggregateType(AggregateType.NONE);
        ColumnDef rechargeMoney = new ColumnDef("recharge_money", TypeDef.createDecimal(32, 2));
        rechargeMoney.setAggregateType(AggregateType.NONE);
        ColumnDef province = new ColumnDef("province", TypeDef.createVarchar(64));
        province.setAggregateType(AggregateType.NONE);
        ColumnDef dt = new ColumnDef("dt", TypeDef.create(PrimitiveType.DATE));
        dt.setAggregateType(AggregateType.NONE);
        return Lists.newArrayList(id, userId, rechargeMoney, province, dt);
    }

    private List<Column> findColumnList() {
        Column id = new Column("id", Type.BIGINT);
        id.setAggregationType(AggregateType.NONE, false);
        Column userId = new Column("user_id", Type.BIGINT);
        userId.setAggregationType(AggregateType.NONE, false);
        Column rechargeMoney = new Column("recharge_money", Type.DECIMAL32);
        rechargeMoney.setAggregationType(AggregateType.NONE, false);
        Column province = new Column("province", Type.VARCHAR);
        province.setAggregationType(AggregateType.NONE, false);
        Column dt = new Column("dt", Type.DATE);
        dt.setAggregationType(AggregateType.NONE, false);
        return Lists.newArrayList(id, userId, rechargeMoney, province, dt);
    }


    private ListPartitionDesc findListMultiPartitionDesc(String colNames, String pName1, String pName2,
                                                         Map<String, String> partitionProperties) {
        List<String> partitionColNames = Lists.newArrayList(colNames.split(","));
        MultiItemListPartitionDesc p1 = new MultiItemListPartitionDesc(false, pName1,
                Lists.newArrayList(Lists.newArrayList("2022-04-15", "guangdong"),
                        Lists.newArrayList("2022-04-15", "tianjin")), partitionProperties);
        MultiItemListPartitionDesc p2 = new MultiItemListPartitionDesc(false, pName2,
                Lists.newArrayList(Lists.newArrayList("2022-04-16", "shanghai"),
                        Lists.newArrayList("2022-04-16", "beijing")), partitionProperties);
        List<PartitionDesc> partitionDescs = Lists.newArrayList(p1, p2);
        return new ListPartitionDesc(partitionColNames, partitionDescs);
    }

    private ListPartitionDesc findListSinglePartitionDesc(String colNames, String pName1, String pName2,
                                                          Map<String, String> partitionProperties) {
        List<String> partitionColNames = Lists.newArrayList(colNames.split(","));
        SingleItemListPartitionDesc p1 = new SingleItemListPartitionDesc(false, pName1,
                Lists.newArrayList("guangdong", "tianjin"), partitionProperties);
        SingleItemListPartitionDesc p2 = new SingleItemListPartitionDesc(false, pName2,
                Lists.newArrayList("shanghai", "beijing"), partitionProperties);
        List<PartitionDesc> partitionDescs = Lists.newArrayList(p1, p2);
        return new ListPartitionDesc(partitionColNames, partitionDescs);
    }

    private Map<String, String> findSupportedProperties(Map<String, String> properties) {
        Map<String, String> supportedProperties = new HashMap<>();
        supportedProperties.put("storage_medium", "SSD");
        supportedProperties.put("replication_num", "1");
        supportedProperties.put("in_memory", "true");
        supportedProperties.put("tablet_type", "memory");
        supportedProperties.put("storage_cooldown_time", "2122-07-09 12:12:12");
        if (properties != null) {
            properties.forEach((k, v) -> supportedProperties.put(k, v));
        }
        return supportedProperties;
    }

    public ListPartitionInfo findSingleListPartitionInfo() throws AnalysisException, DdlException {
        ListPartitionDesc listPartitionDesc = this.findListSinglePartitionDesc("province",
                "p1", "p2", this.findSupportedProperties(null));
        listPartitionDesc.analyze(this.findColumnDefList(), null);

        Map<String, Long> partitionNameToId = new HashMap<>();
        partitionNameToId.put("p1", 10001L);
        partitionNameToId.put("p2", 10002L);
        return (ListPartitionInfo) listPartitionDesc.toPartitionInfo(this.findColumnList(), partitionNameToId, false);
    }

    public ListPartitionInfo findMultiListPartitionInfo() throws AnalysisException, DdlException {
        ListPartitionDesc listPartitionDesc = this.findListMultiPartitionDesc("dt,province",
                "p1", "p2", this.findSupportedProperties(null));
        listPartitionDesc.analyze(this.findColumnDefList(), null);

        Map<String, Long> partitionNameToId = new HashMap<>();
        partitionNameToId.put("p1", 10001L);
        partitionNameToId.put("p2", 10002L);
        return (ListPartitionInfo) listPartitionDesc.toPartitionInfo(this.findColumnList(), partitionNameToId, false);
    }

    @Test
    public void testInvalidValueAndColumnTypeForMultiPartition() {
        assertThrows(AnalysisException.class, () -> {
            List<String> partitionColNames = Lists.newArrayList("dt", "province");
            MultiItemListPartitionDesc p1 = new MultiItemListPartitionDesc(false, "p1",
                    //aaaa is invalid value for a date type column dt, it will throw a DdlException
                    Lists.newArrayList(Lists.newArrayList("aaaa", "guangdong"),
                            Lists.newArrayList("2022-04-15", "tianjin")), null);

            List<PartitionDesc> partitionDescs = Lists.newArrayList(p1);
            ListPartitionDesc listPartitionDesc = new ListPartitionDesc(partitionColNames, partitionDescs);
            listPartitionDesc.analyze(this.findColumnDefList(), null);
        });
    }

    @Test
    public void testInvalidValueAndColumnTypeForSinglePartition() {
        assertThrows(AnalysisException.class, () -> {
            List<String> partitionColNames = Lists.newArrayList("user_id");
            SingleItemListPartitionDesc p1 = new SingleItemListPartitionDesc(false, "p1",
                    //beijing is invalid value for a bigint type column user_id, it will throw a DdlException
                    Lists.newArrayList("guangdong", "beijing"), null);

            List<PartitionDesc> partitionDescs = Lists.newArrayList(p1);
            ListPartitionDesc listPartitionDesc = new ListPartitionDesc(partitionColNames, partitionDescs);
            listPartitionDesc.analyze(this.findColumnDefList(), null);
        });
    }

    @Test
    public void testSingleListPartitionDesc() throws AnalysisException {
        ListPartitionDesc listPartitionDesc = this.findListSinglePartitionDesc("province",
                "p1", "p2", null);
        listPartitionDesc.analyze(this.findColumnDefList(), null);
        String sql = listPartitionDesc.toString();
        String target = "PARTITION BY LIST(`province`)(\n" +
                "  PARTITION p1 VALUES IN (\'guangdong\',\'tianjin\'),\n" +
                "  PARTITION p2 VALUES IN (\'shanghai\',\'beijing\')\n" +
                ")";
        Assertions.assertEquals(sql, target);
    }

    @Test
    public void testMultiListPartitionDesc() throws AnalysisException {
        ListPartitionDesc listPartitionDesc = this.findListMultiPartitionDesc("dt,province",
                "p1", "p2", null);
        listPartitionDesc.analyze(this.findColumnDefList(), null);
        String sql = listPartitionDesc.toString();
        String target = "PARTITION BY LIST(`dt`,`province`)(\n" +
                "  PARTITION p1 VALUES IN ((\'2022-04-15\',\'guangdong\'),(\'2022-04-15\',\'tianjin\')),\n" +
                "  PARTITION p2 VALUES IN ((\'2022-04-16\',\'shanghai\'),(\'2022-04-16\',\'beijing\'))\n" +
                ")";
        Assertions.assertEquals(sql, target);
    }

    @Test
    public void testDuplicatePartitionColumn() {
        assertThrows(AnalysisException.class, () -> {
            ListPartitionDesc listPartitionDesc = this.findListSinglePartitionDesc("dt,dt", "p1", "p1", null);
            listPartitionDesc.analyze(this.findColumnDefList(), null);
        });
    }

    @Test
    public void testNotAggregatedColumn() {
        assertThrows(AnalysisException.class, () -> {
            ColumnDef province = new ColumnDef("province", TypeDef.createVarchar(64));
            province.setAggregateType(AggregateType.MAX);
            ColumnDef dt = new ColumnDef("dt", TypeDef.createVarchar(10));
            dt.setAggregateType(AggregateType.NONE);
            List<ColumnDef> columnDefList = Lists.newArrayList(province, dt);
            ListPartitionDesc listSinglePartitionDesc = this.findListSinglePartitionDesc("province", "p1", "p2", null);
            listSinglePartitionDesc.analyze(columnDefList, null);
        });
    }

    @Test
    public void testColumnNoExist() {
        assertThrows(AnalysisException.class, () -> {
            ListPartitionDesc listPartitionDesc = this.findListSinglePartitionDesc("name", "p1", "p1", null);
            listPartitionDesc.analyze(this.findColumnDefList(), null);
        });
    }

    @Test
    public void testDuplicateSingleListPartitionNames() {
        assertThrows(AnalysisException.class, () -> {
            ListPartitionDesc listSinglePartitionDesc = this.findListSinglePartitionDesc("province", "p1", "p1", null);
            listSinglePartitionDesc.analyze(this.findColumnDefList(), null);
        });
    }

    @Test
    public void testDuplicateMultiListPartitionNames() {
        assertThrows(AnalysisException.class, () -> {
            ListPartitionDesc listMultiPartitionDesc = this.findListMultiPartitionDesc("dt,province", "p1", "p1", null);
            listMultiPartitionDesc.analyze(this.findColumnDefList(), null);
        });
    }

    @Test
    public void testStorageMedium() {
        assertThrows(AnalysisException.class, () -> {
            Map<String, String> supportedProperties = new HashMap<>();
            supportedProperties.put("storage_medium", "xxx");
            ListPartitionDesc listMultiPartitionDesc = this.findListMultiPartitionDesc(
                    "dt,province", "p1", "p1", this.findSupportedProperties(supportedProperties));
            listMultiPartitionDesc.analyze(this.findColumnDefList(), this.findSupportedProperties(null));
        });
    }

    @Test
    public void testCoolDownTime() {
        assertThrows(AnalysisException.class, () -> {
            Map<String, String> supportedProperties = new HashMap<>();
            supportedProperties.put("storage_cooldown_time", "2021-04-01 12:12:12");
            ListPartitionDesc listMultiPartitionDesc = this.findListMultiPartitionDesc(
                    "dt,province", "p1", "p1", this.findSupportedProperties(supportedProperties));
            listMultiPartitionDesc.analyze(this.findColumnDefList(), this.findSupportedProperties(null));
        });
    }

    @Test
    public void testReplicaNum() {
        assertThrows(SemanticException.class, () -> {
            Map<String, String> supportedProperties = new HashMap<>();
            supportedProperties.put("replication_num", "0");
            ListPartitionDesc listMultiPartitionDesc = this.findListMultiPartitionDesc(
                    "dt,province", "p1", "p1", this.findSupportedProperties(supportedProperties));
            listMultiPartitionDesc.analyze(this.findColumnDefList(), this.findSupportedProperties(null));
        });
    }

    @Test
    public void testIsInMemory() {
        assertThrows(AnalysisException.class, () -> {
            Map<String, String> supportedProperties = new HashMap<>();
            supportedProperties.put("in_memory", "xxx");
            ListPartitionDesc listMultiPartitionDesc = this.findListMultiPartitionDesc(
                    "dt,province", "p1", "p1", this.findSupportedProperties(supportedProperties));
            listMultiPartitionDesc.analyze(this.findColumnDefList(), this.findSupportedProperties(null));
        });
    }

    @Test
    public void testUnSupportProperties() {
        assertThrows(AnalysisException.class, () -> {
            Map<String, String> supportedProperties = new HashMap<>();
            supportedProperties.put("no_support", "xxx");
            ListPartitionDesc listMultiPartitionDesc = this.findListMultiPartitionDesc(
                    "dt,province", "p1", "p1", this.findSupportedProperties(supportedProperties));
            listMultiPartitionDesc.analyze(this.findColumnDefList(), null);
        });
    }

    @Test
    public void testToPartitionInfoForSingle() throws AnalysisException, DdlException, ParseException {
        ListPartitionInfo partitionInfo = this.findSingleListPartitionInfo();
        Assertions.assertEquals(PartitionType.LIST, partitionInfo.getType());

        DataProperty dataProperty = partitionInfo.getDataProperty(10001L);
        Assertions.assertEquals(TStorageMedium.SSD, dataProperty.getStorageMedium());
        DateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long time = sf.parse("2122-07-09 12:12:12").getTime();
        Assertions.assertEquals(time, dataProperty.getCooldownTimeMs());

        Assertions.assertEquals(1, partitionInfo.getReplicationNum(10001L));
        Assertions.assertEquals(TTabletType.TABLET_TYPE_MEMORY, partitionInfo.getTabletType(10001L));
        Assertions.assertEquals(true, partitionInfo.getIsInMemory(10001L));
        Assertions.assertEquals(false, partitionInfo.isMultiColumnPartition());
    }

    @Test
    public void testToPartitionInfoForMulti() throws AnalysisException, DdlException, ParseException {
        ListPartitionInfo partitionInfo = this.findMultiListPartitionInfo();
        Assertions.assertEquals(PartitionType.LIST, partitionInfo.getType());

        DataProperty dataProperty = partitionInfo.getDataProperty(10001L);
        Assertions.assertEquals(TStorageMedium.SSD, dataProperty.getStorageMedium());
        DateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long time = sf.parse("2122-07-09 12:12:12").getTime();
        Assertions.assertEquals(time, dataProperty.getCooldownTimeMs());

        Assertions.assertEquals(1, partitionInfo.getReplicationNum(10001L));
        Assertions.assertEquals(TTabletType.TABLET_TYPE_MEMORY, partitionInfo.getTabletType(10001L));
        Assertions.assertEquals(true, partitionInfo.getIsInMemory(10001L));
        Assertions.assertEquals(true, partitionInfo.isMultiColumnPartition());
    }

    /**
     * test duplicate value in same partition
     *
     * @throws AnalysisException
     */
    @Test
    public void testMultiPartitionDuplicatedValue1() {
        assertThrows(AnalysisException.class, () -> {
            List<String> partitionColNames = Lists.newArrayList("dt", "province");
            MultiItemListPartitionDesc p1 = new MultiItemListPartitionDesc(false, "p1",
                    Lists.newArrayList(Lists.newArrayList("2022-04-15", "guangdong"),
                            Lists.newArrayList("2022-04-15", "guangdong")), null);
            MultiItemListPartitionDesc p2 = new MultiItemListPartitionDesc(false, "p2",
                    Lists.newArrayList(Lists.newArrayList("2022-04-16", "shanghai"),
                            Lists.newArrayList("2022-04-15", "beijing")), null);
            List<PartitionDesc> partitionDescs = Lists.newArrayList(p1, p2);
            ListPartitionDesc listPartitionDesc = new ListPartitionDesc(partitionColNames, partitionDescs);
            listPartitionDesc.analyze(this.findColumnDefList(), null);
        });
    }

    /**
     * test duplicate value in different partition
     *
     * @throws AnalysisException
     */
    @Test
    public void testMultiPartitionDuplicatedValue2() {
        assertThrows(AnalysisException.class, () -> {
            List<String> partitionColNames = Lists.newArrayList("dt", "province");
            MultiItemListPartitionDesc p1 = new MultiItemListPartitionDesc(false, "p1",
                    Lists.newArrayList(Lists.newArrayList("2022-04-15", "guangdong"),
                            Lists.newArrayList("2022-04-15", "tianjin")), null);
            MultiItemListPartitionDesc p2 = new MultiItemListPartitionDesc(false, "p2",
                    Lists.newArrayList(Lists.newArrayList("2022-04-16", "shanghai"),
                            Lists.newArrayList("2022-04-15", "guangdong")), null);
            List<PartitionDesc> partitionDescs = Lists.newArrayList(p1, p2);
            ListPartitionDesc listPartitionDesc = new ListPartitionDesc(partitionColNames, partitionDescs);
            listPartitionDesc.analyze(this.findColumnDefList(), null);
        });
    }

    /**
     * test duplicate value in same partition
     *
     * @throws AnalysisException
     */
    @Test
    public void testSinglePartitionDuplicatedValue1() {
        assertThrows(AnalysisException.class, () -> {
            List<String> partitionColNames = Lists.newArrayList("province");
            SingleItemListPartitionDesc p1 = new SingleItemListPartitionDesc(false, "p1",
                    Lists.newArrayList("guangdong", "guangdong"), null);
            SingleItemListPartitionDesc p2 = new SingleItemListPartitionDesc(false, "p2",
                    Lists.newArrayList("shanghai", "beijing"), null);
            List<PartitionDesc> partitionDescs = Lists.newArrayList(p1, p2);
            ListPartitionDesc listPartitionDesc = new ListPartitionDesc(partitionColNames, partitionDescs);
            listPartitionDesc.analyze(this.findColumnDefList(), null);
        });
    }

    /**
     * test duplicate value in different partition
     *
     * @throws AnalysisException
     */
    @Test
    public void testSinglePartitionDuplicatedValue2() {
        assertThrows(AnalysisException.class, () -> {
            List<String> partitionColNames = Lists.newArrayList("province");
            SingleItemListPartitionDesc p1 = new SingleItemListPartitionDesc(false, "p1",
                    Lists.newArrayList("guangdong", "beijing"), null);
            SingleItemListPartitionDesc p2 = new SingleItemListPartitionDesc(false, "p2",
                    Lists.newArrayList("shanghai", "beijing"), null);
            List<PartitionDesc> partitionDescs = Lists.newArrayList(p1, p2);
            ListPartitionDesc listPartitionDesc = new ListPartitionDesc(partitionColNames, partitionDescs);
            listPartitionDesc.analyze(this.findColumnDefList(), null);
        });
    }

    @Test
    public void testCheckHivePartitionColumns() {
        List<String> partitionNames = Lists.newArrayList("p1");
        ColumnDef columnDef = new ColumnDef("p1", TypeDef.create(PrimitiveType.INT));
        ListPartitionDesc listPartitionDesc = new ListPartitionDesc(partitionNames, new ArrayList<>());
        ExceptionChecker.expectThrowsWithMsg(SemanticException.class,
                "Table contains only partition columns",
                () -> listPartitionDesc.checkHivePartitionColPos(Lists.newArrayList(columnDef)));

        partitionNames = Lists.newArrayList("p1", "p2");
        List<ColumnDef> columnDefs = Lists.newArrayList(
                new ColumnDef("c1", TypeDef.create(PrimitiveType.INT)),
                new ColumnDef("p2", TypeDef.create(PrimitiveType.INT)),
                new ColumnDef("p1", TypeDef.create(PrimitiveType.INT)));
        ListPartitionDesc listPartitionDesc1 = new ListPartitionDesc(partitionNames, new ArrayList<>());

        ExceptionChecker.expectThrowsWithMsg(SemanticException.class,
                "Partition columns must be the last columns in the table and in the same order as partition by clause:",
                () -> listPartitionDesc1.checkHivePartitionColPos(Lists.newArrayList(columnDefs)));

        ListPartitionDesc listPartitionDesc2 = new ListPartitionDesc(new ArrayList<>(), new ArrayList<>());

        ExceptionChecker.expectThrowsWithMsg(SemanticException.class,
                "No partition columns",
                () -> listPartitionDesc2.analyzeExternalPartitionColumns(new ArrayList<>(), ""));

        partitionNames = Lists.newArrayList("p1");
        ListPartitionDesc listPartitionDesc3 = new ListPartitionDesc(partitionNames, new ArrayList<>());

        ExceptionChecker.expectThrowsWithMsg(SemanticException.class,
                "Partition column[p1] does not exist in column list",
                () -> listPartitionDesc3.analyzeExternalPartitionColumns(new ArrayList<>(), ""));

        ColumnDef columnDef1 = new ColumnDef("p1", TypeDef.create(PrimitiveType.INT));
        partitionNames = Lists.newArrayList("p1", "p1");
        ListPartitionDesc listPartitionDesc4 = new ListPartitionDesc(partitionNames, new ArrayList<>());

        ExceptionChecker.expectThrowsWithMsg(SemanticException.class,
                "Duplicated partition column",
                () -> listPartitionDesc4.analyzeExternalPartitionColumns(Lists.newArrayList(columnDef1), ""));

        partitionNames = Lists.newArrayList("p1");
        List<ColumnDef> columnDefs1 = Lists.newArrayList(
                new ColumnDef("c1", TypeDef.create(PrimitiveType.INT)),
                new ColumnDef("p1", TypeDef.create(PrimitiveType.DECIMAL32)));
        ListPartitionDesc listPartitionDesc5 = new ListPartitionDesc(partitionNames, new ArrayList<>());

        ExceptionChecker.expectThrowsWithMsg(SemanticException.class,
                "Invalid partition column",
                () -> listPartitionDesc5.analyzeExternalPartitionColumns(columnDefs1, "hive"));

        partitionNames = Lists.newArrayList("p1");
        List<ColumnDef> columnDefs2 = Lists.newArrayList(
                new ColumnDef("c1", TypeDef.create(PrimitiveType.INT)),
                new ColumnDef("p1", TypeDef.create(PrimitiveType.INT)));
        ListPartitionDesc listPartitionDesc6 = new ListPartitionDesc(partitionNames, new ArrayList<>());
        listPartitionDesc6.analyzeExternalPartitionColumns(columnDefs2, "hive");
    }
}
