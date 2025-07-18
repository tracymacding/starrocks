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


package com.starrocks.sql.optimizer.rule;

import com.google.common.base.Stopwatch;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.sql.optimizer.GroupExpression;
import com.starrocks.sql.optimizer.Memo;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.MockOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class BinderTest {

    private Binder buildBinder(Pattern pattern, OptExpression expr) {
        Memo memo = new Memo();
        OptimizerContext optimizerContext = OptimizerFactory.mockContext(new ColumnRefFactory());
        Stopwatch stopwatch = Stopwatch.createStarted();
        return new Binder(optimizerContext, pattern, memo.init(expr), stopwatch);
    }

    private Binder buildBinder(Pattern pattern, GroupExpression qe) {
        Memo memo = new Memo();
        OptimizerContext optimizerContext = OptimizerFactory.mockContext(new ColumnRefFactory());
        Stopwatch stopwatch = Stopwatch.createStarted();
        return new Binder(optimizerContext, pattern, qe, stopwatch);
    }

    private OptExpression bindNext(Pattern pattern, OptExpression expr) {
        Memo memo = new Memo();
        OptimizerContext optimizerContext = OptimizerFactory.mockContext(new ColumnRefFactory());
        Stopwatch stopwatch = Stopwatch.createStarted();
        Binder binder = new Binder(optimizerContext, pattern, memo.init(expr), stopwatch);
        return binder.next();
    }

    @Test
    public void testBinder() {
        OlapTable table1 = new OlapTable();
        table1.setDefaultDistributionInfo(new HashDistributionInfo());
        OlapTable table2 = new OlapTable();
        table2.setDefaultDistributionInfo(new HashDistributionInfo());
        OptExpression expr = OptExpression.create(new LogicalJoinOperator(),
                new OptExpression(new LogicalOlapScanOperator(table1)),
                new OptExpression(new LogicalOlapScanOperator(table2)));

        Pattern pattern = Pattern.create(OperatorType.LOGICAL_JOIN)
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF))
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF));

        OptExpression result = bindNext(pattern, expr);

        assertEquals(OperatorType.LOGICAL_JOIN, result.getOp().getOpType());
        assertEquals(OperatorType.LOGICAL_OLAP_SCAN, result.inputAt(0).getOp().getOpType());
        assertEquals(OperatorType.LOGICAL_OLAP_SCAN, result.inputAt(1).getOp().getOpType());
    }

    @Test
    public void testBinder2() {
        OptExpression expr = OptExpression.create(new MockOperator(OperatorType.LOGICAL_JOIN),
                new OptExpression(new MockOperator(OperatorType.LOGICAL_OLAP_SCAN)),
                new OptExpression(new MockOperator(OperatorType.LOGICAL_OLAP_SCAN)));

        Pattern pattern = Pattern.create(OperatorType.PATTERN_LEAF)
                .addChildren(Pattern.create(OperatorType.LOGICAL_JOIN))
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF));

        OptExpression result = bindNext(pattern, expr);

        assertNull(result);
    }

    @Test
    public void testBinderTop() {
        OptExpression expr = OptExpression.create(new MockOperator(OperatorType.LOGICAL_JOIN),
                new OptExpression(new MockOperator(OperatorType.LOGICAL_OLAP_SCAN)),
                new OptExpression(new MockOperator(OperatorType.LOGICAL_OLAP_SCAN)));

        Pattern pattern = Pattern.create(OperatorType.LOGICAL_JOIN);

        OptExpression result = bindNext(pattern, expr);

        assertEquals(OperatorType.LOGICAL_JOIN, result.getOp().getOpType());
    }



    @Test
    public void testBinderOne() {
        OptExpression expr = OptExpression.create(new MockOperator(OperatorType.LOGICAL_JOIN));
        Pattern pattern = Pattern.create(OperatorType.LOGICAL_JOIN);
        Binder binder = buildBinder(pattern, expr);
        OptExpression result = binder.next();

        assertEquals(OperatorType.LOGICAL_JOIN, result.getOp().getOpType());

        assertNull(binder.next());
    }

    @Test
    public void testBinder3() {
        OptExpression expr = OptExpression.create(new MockOperator(OperatorType.LOGICAL_OLAP_SCAN),
                new OptExpression(new MockOperator(OperatorType.LOGICAL_JOIN)),
                new OptExpression(new MockOperator(OperatorType.LOGICAL_OLAP_SCAN)));

        Pattern pattern = Pattern.create(OperatorType.PATTERN_LEAF)
                .addChildren(Pattern.create(OperatorType.LOGICAL_JOIN))
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF));

        OptExpression result = bindNext(pattern, expr);

        assertEquals(OperatorType.LOGICAL_OLAP_SCAN, result.getOp().getOpType());
        assertEquals(OperatorType.LOGICAL_JOIN, result.inputAt(0).getOp().getOpType());
        assertEquals(OperatorType.LOGICAL_OLAP_SCAN, result.inputAt(1).getOp().getOpType());
    }

    @Test
    public void testBinderDepth3() {
        OptExpression expr = OptExpression.create(new MockOperator(OperatorType.LOGICAL_PROJECT),
                OptExpression.create(new MockOperator(OperatorType.LOGICAL_JOIN),
                        OptExpression.create(new MockOperator(OperatorType.LOGICAL_OLAP_SCAN, 1)),
                        OptExpression.create(new MockOperator(OperatorType.LOGICAL_PROJECT))),
                OptExpression.create(new MockOperator(OperatorType.LOGICAL_JOIN),
                        OptExpression.create(new MockOperator(OperatorType.LOGICAL_OLAP_SCAN, 2)),
                        OptExpression.create(new MockOperator(OperatorType.LOGICAL_OLAP_SCAN, 3))));

        Pattern pattern1 = Pattern.create(OperatorType.PATTERN_LEAF)
                .addChildren(Pattern.create(OperatorType.LOGICAL_JOIN))
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF));

        OptExpression result = bindNext(pattern1, expr);

        assertEquals(OperatorType.LOGICAL_PROJECT, result.getOp().getOpType());
        assertEquals(OperatorType.LOGICAL_JOIN, result.inputAt(0).getOp().getOpType());
        assertEquals(OperatorType.LOGICAL_JOIN, result.inputAt(1).getOp().getOpType());

        Pattern pattern2 = Pattern.create(OperatorType.LOGICAL_JOIN)
                .addChildren(Pattern.create(OperatorType.LOGICAL_JOIN))
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF));

        assertNull(bindNext(pattern2, expr));

        Pattern pattern3 = Pattern.create(OperatorType.LOGICAL_PROJECT)
                .addChildren(Pattern.create(OperatorType.LOGICAL_JOIN).addChildren(
                        Pattern.create(OperatorType.PATTERN_LEAF),
                        Pattern.create(OperatorType.PATTERN_LEAF)))
                .addChildren(Pattern.create(OperatorType.LOGICAL_JOIN));

        result = bindNext(pattern3, expr);

        assertEquals(OperatorType.LOGICAL_PROJECT, result.getOp().getOpType());
        assertEquals(OperatorType.LOGICAL_JOIN, result.inputAt(0).getOp().getOpType());
        assertEquals(OperatorType.LOGICAL_JOIN, result.inputAt(1).getOp().getOpType());
    }

    @Test
    public void testBinderDepth2Repeat4() {
        OptExpression expr1 = OptExpression.create(new MockOperator(OperatorType.LOGICAL_JOIN, 0),
                OptExpression.create(new MockOperator(OperatorType.LOGICAL_OLAP_SCAN, 1)),
                OptExpression.create(new MockOperator(OperatorType.LOGICAL_OLAP_SCAN, 2)));

        OptExpression expr2 = OptExpression.create(new MockOperator(OperatorType.LOGICAL_OLAP_SCAN, 3));
        OptExpression expr3 = OptExpression.create(new MockOperator(OperatorType.LOGICAL_OLAP_SCAN, 4));

        Memo memo = new Memo();
        GroupExpression ge = memo.init(expr1);

        memo.copyIn(ge.inputAt(0), expr2);
        memo.copyIn(ge.inputAt(1), expr3);

        Pattern pattern = Pattern.create(OperatorType.LOGICAL_JOIN)
                .addChildren(Pattern.create(OperatorType.LOGICAL_OLAP_SCAN))
                .addChildren(Pattern.create(OperatorType.LOGICAL_OLAP_SCAN));

        Binder binder = buildBinder(pattern, ge);
        OptExpression result;

        result = binder.next();
        assertEquals(OperatorType.LOGICAL_JOIN, result.getOp().getOpType());
        assertEquals(OperatorType.LOGICAL_OLAP_SCAN, result.inputAt(0).getOp().getOpType());
        assertEquals(1, ((MockOperator) result.inputAt(0).getOp()).getValue());

        assertEquals(OperatorType.LOGICAL_OLAP_SCAN, result.inputAt(1).getOp().getOpType());
        assertEquals(2, ((MockOperator) result.inputAt(1).getOp()).getValue());

        result = binder.next();
        assertEquals(OperatorType.LOGICAL_JOIN, result.getOp().getOpType());
        assertEquals(OperatorType.LOGICAL_OLAP_SCAN, result.inputAt(0).getOp().getOpType());
        assertEquals(1, ((MockOperator) result.inputAt(0).getOp()).getValue());

        assertEquals(OperatorType.LOGICAL_OLAP_SCAN, result.inputAt(1).getOp().getOpType());
        assertEquals(4, ((MockOperator) result.inputAt(1).getOp()).getValue());

        result = binder.next();
        assertEquals(OperatorType.LOGICAL_JOIN, result.getOp().getOpType());
        assertEquals(OperatorType.LOGICAL_OLAP_SCAN, result.inputAt(0).getOp().getOpType());
        assertEquals(3, ((MockOperator) result.inputAt(0).getOp()).getValue());

        assertEquals(OperatorType.LOGICAL_OLAP_SCAN, result.inputAt(1).getOp().getOpType());
        assertEquals(2, ((MockOperator) result.inputAt(1).getOp()).getValue());

        result = binder.next();
        assertEquals(OperatorType.LOGICAL_JOIN, result.getOp().getOpType());
        assertEquals(OperatorType.LOGICAL_OLAP_SCAN, result.inputAt(0).getOp().getOpType());
        assertEquals(3, ((MockOperator) result.inputAt(0).getOp()).getValue());

        assertEquals(OperatorType.LOGICAL_OLAP_SCAN, result.inputAt(1).getOp().getOpType());
        assertEquals(4, ((MockOperator) result.inputAt(1).getOp()).getValue());

        assertNull(binder.next());
    }

    @Test
    public void testBinderDepth2Repeat2() {
        OptExpression expr1 = OptExpression.create(new MockOperator(OperatorType.LOGICAL_JOIN, 0),
                OptExpression.create(new MockOperator(OperatorType.LOGICAL_OLAP_SCAN, 1)),
                OptExpression.create(new MockOperator(OperatorType.LOGICAL_OLAP_SCAN, 2)));

        OptExpression expr2 = OptExpression.create(new MockOperator(OperatorType.LOGICAL_OLAP_SCAN, 3));
        OptExpression expr3 = OptExpression.create(new MockOperator(OperatorType.LOGICAL_OLAP_SCAN, 4));

        Memo memo = new Memo();
        GroupExpression ge = memo.init(expr1);

        memo.copyIn(ge.inputAt(0), expr2);
        memo.copyIn(ge.inputAt(1), expr3);

        Pattern pattern = Pattern.create(OperatorType.LOGICAL_JOIN)
                .addChildren(Pattern.create(OperatorType.LOGICAL_OLAP_SCAN))
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF));

        Binder binder = buildBinder(pattern, ge);
        OptExpression result;

        result = binder.next();
        assertEquals(OperatorType.LOGICAL_JOIN, result.getOp().getOpType());
        assertEquals(OperatorType.LOGICAL_OLAP_SCAN, result.inputAt(0).getOp().getOpType());
        assertEquals(1, ((MockOperator) result.inputAt(0).getOp()).getValue());

        assertEquals(OperatorType.LOGICAL_OLAP_SCAN, result.inputAt(1).getOp().getOpType());
        assertEquals(2, ((MockOperator) result.inputAt(1).getOp()).getValue());

        result = binder.next();
        assertEquals(OperatorType.LOGICAL_JOIN, result.getOp().getOpType());
        assertEquals(OperatorType.LOGICAL_OLAP_SCAN, result.inputAt(0).getOp().getOpType());
        assertEquals(3, ((MockOperator) result.inputAt(0).getOp()).getValue());

        assertEquals(OperatorType.LOGICAL_OLAP_SCAN, result.inputAt(1).getOp().getOpType());
        assertEquals(2, ((MockOperator) result.inputAt(1).getOp()).getValue());

        assertNull(binder.next());
    }

    @Test
    public void testBinderDepth2Repeat1() {
        OptExpression expr1 = OptExpression.create(new MockOperator(OperatorType.LOGICAL_JOIN, 0),
                OptExpression.create(new MockOperator(OperatorType.LOGICAL_OLAP_SCAN, 1)),
                OptExpression.create(new MockOperator(OperatorType.LOGICAL_OLAP_SCAN, 2)));

        OptExpression expr2 = OptExpression.create(new MockOperator(OperatorType.LOGICAL_OLAP_SCAN, 3));
        OptExpression expr3 = OptExpression.create(new MockOperator(OperatorType.LOGICAL_OLAP_SCAN, 4));

        Memo memo = new Memo();
        GroupExpression ge = memo.init(expr1);

        memo.copyIn(ge.inputAt(0), expr2);
        memo.copyIn(ge.inputAt(1), expr3);

        Pattern pattern = Pattern.create(OperatorType.LOGICAL_JOIN)
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF))
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF));

        Binder binder = buildBinder(pattern, expr1);
        OptExpression result;

        result = binder.next();
        assertEquals(OperatorType.LOGICAL_JOIN, result.getOp().getOpType());

        assertEquals(OperatorType.LOGICAL_OLAP_SCAN, result.inputAt(0).getOp().getOpType());
        assertEquals(1, ((MockOperator) result.inputAt(0).getOp()).getValue());

        assertEquals(OperatorType.LOGICAL_OLAP_SCAN, result.inputAt(1).getOp().getOpType());
        assertEquals(2, ((MockOperator) result.inputAt(1).getOp()).getValue());

        assertNull(binder.next());
    }

    @Test
    public void testBinderMulti() {
        OptExpression expr1 = OptExpression.create(new MockOperator(OperatorType.LOGICAL_JOIN, 0),
                OptExpression.create(new MockOperator(OperatorType.LOGICAL_OLAP_SCAN, 1)),
                OptExpression.create(new MockOperator(OperatorType.LOGICAL_OLAP_SCAN, 2)),
                OptExpression.create(new MockOperator(OperatorType.LOGICAL_OLAP_SCAN, 3)),
                OptExpression.create(new MockOperator(OperatorType.LOGICAL_OLAP_SCAN, 4)),
                OptExpression.create(new MockOperator(OperatorType.LOGICAL_PROJECT, 5)));

        Memo memo = new Memo();
        GroupExpression ge = memo.init(expr1);

        Pattern pattern = Pattern.create(OperatorType.LOGICAL_JOIN)
                .addChildren(Pattern.create(OperatorType.PATTERN_MULTI_LEAF));

        Binder binder = buildBinder(pattern, expr1);
        OptExpression result;

        result = binder.next();
        assertEquals(OperatorType.LOGICAL_JOIN, result.getOp().getOpType());

        assertEquals(OperatorType.LOGICAL_OLAP_SCAN, result.inputAt(0).getOp().getOpType());
        assertEquals(1, ((MockOperator) result.inputAt(0).getOp()).getValue());

        assertEquals(OperatorType.LOGICAL_OLAP_SCAN, result.inputAt(1).getOp().getOpType());
        assertEquals(2, ((MockOperator) result.inputAt(1).getOp()).getValue());

        assertEquals(OperatorType.LOGICAL_OLAP_SCAN, result.inputAt(2).getOp().getOpType());
        assertEquals(3, ((MockOperator) result.inputAt(2).getOp()).getValue());

        assertEquals(OperatorType.LOGICAL_OLAP_SCAN, result.inputAt(3).getOp().getOpType());
        assertEquals(4, ((MockOperator) result.inputAt(3).getOp()).getValue());

        assertEquals(OperatorType.LOGICAL_PROJECT, result.inputAt(4).getOp().getOpType());
        assertEquals(5, ((MockOperator) result.inputAt(4).getOp()).getValue());

        assertNull(binder.next());
    }

    @Test
    public void testBinderMulti2() {
        OptExpression expr1 = OptExpression.create(new MockOperator(OperatorType.LOGICAL_JOIN, 0),
                OptExpression.create(new MockOperator(OperatorType.LOGICAL_PROJECT, 1)),
                OptExpression.create(new MockOperator(OperatorType.LOGICAL_OLAP_SCAN, 2)),
                OptExpression.create(new MockOperator(OperatorType.LOGICAL_OLAP_SCAN, 3)),
                OptExpression.create(new MockOperator(OperatorType.LOGICAL_OLAP_SCAN, 4)),
                OptExpression.create(new MockOperator(OperatorType.LOGICAL_PROJECT, 5)));

        Memo memo = new Memo();
        GroupExpression ge = memo.init(expr1);

        Pattern pattern = Pattern.create(OperatorType.LOGICAL_JOIN)
                .addChildren(Pattern.create(OperatorType.LOGICAL_PROJECT))
                .addChildren(Pattern.create(OperatorType.PATTERN_MULTI_LEAF))
                .addChildren(Pattern.create(OperatorType.LOGICAL_OLAP_SCAN));

        Binder binder = buildBinder(pattern, expr1);
        assertNull(binder.next());
    }

    @Test
    public void testBinderMulti3() {
        OptExpression expr1 = OptExpression.create(new MockOperator(OperatorType.LOGICAL_JOIN, 0),
                OptExpression.create(new MockOperator(OperatorType.LOGICAL_PROJECT, 1)),
                OptExpression.create(new MockOperator(OperatorType.LOGICAL_OLAP_SCAN, 2)),
                OptExpression.create(new MockOperator(OperatorType.LOGICAL_OLAP_SCAN, 3)),
                OptExpression.create(new MockOperator(OperatorType.LOGICAL_OLAP_SCAN, 4)),
                OptExpression.create(new MockOperator(OperatorType.LOGICAL_PROJECT, 5)));

        Memo memo = new Memo();
        GroupExpression ge = memo.init(expr1);

        Pattern pattern = Pattern.create(OperatorType.LOGICAL_JOIN)
                .addChildren(Pattern.create(OperatorType.LOGICAL_PROJECT))
                .addChildren(Pattern.create(OperatorType.PATTERN_MULTI_LEAF))
                .addChildren(Pattern.create(OperatorType.LOGICAL_PROJECT));

        Binder binder = buildBinder(pattern, expr1);
        OptExpression result;

        result = binder.next();
        assertEquals(OperatorType.LOGICAL_JOIN, result.getOp().getOpType());

        assertEquals(OperatorType.LOGICAL_PROJECT, result.inputAt(0).getOp().getOpType());
        assertEquals(1, ((MockOperator) result.inputAt(0).getOp()).getValue());

        assertEquals(OperatorType.LOGICAL_OLAP_SCAN, result.inputAt(1).getOp().getOpType());
        assertEquals(2, ((MockOperator) result.inputAt(1).getOp()).getValue());

        assertEquals(OperatorType.LOGICAL_OLAP_SCAN, result.inputAt(2).getOp().getOpType());
        assertEquals(3, ((MockOperator) result.inputAt(2).getOp()).getValue());

        assertEquals(OperatorType.LOGICAL_OLAP_SCAN, result.inputAt(3).getOp().getOpType());
        assertEquals(4, ((MockOperator) result.inputAt(3).getOp()).getValue());

        assertEquals(OperatorType.LOGICAL_PROJECT, result.inputAt(4).getOp().getOpType());
        assertEquals(5, ((MockOperator) result.inputAt(4).getOp()).getValue());

        assertNull(binder.next());
    }

    @Test
    public void testBinderMultiDepth2Repeat1() {
        OptExpression expr1 = OptExpression.create(new MockOperator(OperatorType.LOGICAL_JOIN, 0),
                OptExpression.create(new MockOperator(OperatorType.LOGICAL_OLAP_SCAN, 1)),
                OptExpression.create(new MockOperator(OperatorType.LOGICAL_OLAP_SCAN, 2)));

        OptExpression expr2 = OptExpression.create(new MockOperator(OperatorType.LOGICAL_OLAP_SCAN, 3));
        OptExpression expr3 = OptExpression.create(new MockOperator(OperatorType.LOGICAL_OLAP_SCAN, 4));

        Memo memo = new Memo();
        GroupExpression ge = memo.init(expr1);

        memo.copyIn(ge.inputAt(0), expr2);
        memo.copyIn(ge.inputAt(1), expr3);

        Pattern pattern = Pattern.create(OperatorType.LOGICAL_JOIN)
                .addChildren(Pattern.create(OperatorType.PATTERN_MULTI_LEAF));

        Binder binder = buildBinder(pattern, expr1);
        OptExpression result;

        result = binder.next();
        assertEquals(OperatorType.LOGICAL_JOIN, result.getOp().getOpType());

        assertEquals(OperatorType.LOGICAL_OLAP_SCAN, result.inputAt(0).getOp().getOpType());
        assertEquals(1, ((MockOperator) result.inputAt(0).getOp()).getValue());

        assertEquals(OperatorType.LOGICAL_OLAP_SCAN, result.inputAt(1).getOp().getOpType());
        assertEquals(2, ((MockOperator) result.inputAt(1).getOp()).getValue());

        assertNull(binder.next());
    }

    @Test
    public void testBinderMultiDepth2Repeat2() {
        OptExpression expr1 = OptExpression.create(new MockOperator(OperatorType.LOGICAL_JOIN, 0),
                OptExpression.create(new MockOperator(OperatorType.LOGICAL_OLAP_SCAN, 1)),
                OptExpression.create(new MockOperator(OperatorType.LOGICAL_OLAP_SCAN, 2)));

        OptExpression expr2 = OptExpression.create(new MockOperator(OperatorType.LOGICAL_OLAP_SCAN, 3));
        OptExpression expr3 = OptExpression.create(new MockOperator(OperatorType.LOGICAL_OLAP_SCAN, 4));

        Memo memo = new Memo();
        GroupExpression ge = memo.init(expr1);

        memo.copyIn(ge.inputAt(0), expr2);
        memo.copyIn(ge.inputAt(1), expr3);

        Pattern pattern = Pattern.create(OperatorType.LOGICAL_JOIN)
                .addChildren(Pattern.create(OperatorType.LOGICAL_OLAP_SCAN))
                .addChildren(Pattern.create(OperatorType.PATTERN_MULTI_LEAF));

        Binder binder = buildBinder(pattern, ge);
        OptExpression result;

        result = binder.next();
        assertEquals(OperatorType.LOGICAL_JOIN, result.getOp().getOpType());
        assertEquals(OperatorType.LOGICAL_OLAP_SCAN, result.inputAt(0).getOp().getOpType());
        assertEquals(1, ((MockOperator) result.inputAt(0).getOp()).getValue());

        assertEquals(OperatorType.LOGICAL_OLAP_SCAN, result.inputAt(1).getOp().getOpType());
        assertEquals(2, ((MockOperator) result.inputAt(1).getOp()).getValue());

        result = binder.next();
        assertEquals(OperatorType.LOGICAL_JOIN, result.getOp().getOpType());
        assertEquals(OperatorType.LOGICAL_OLAP_SCAN, result.inputAt(0).getOp().getOpType());
        assertEquals(3, ((MockOperator) result.inputAt(0).getOp()).getValue());

        assertEquals(OperatorType.LOGICAL_OLAP_SCAN, result.inputAt(1).getOp().getOpType());
        assertEquals(2, ((MockOperator) result.inputAt(1).getOp()).getValue());

        assertNull(binder.next());
    }

}
