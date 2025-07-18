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

package com.starrocks.load.routineload;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.KafkaUtil;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class KafkaTaskInfoTest {

    @Test
    public void testReadyToExecute(@Injectable KafkaRoutineLoadJob kafkaRoutineLoadJob) throws Exception {
        new MockUp<RoutineLoadMgr>() {
            @Mock
            public RoutineLoadJob getJob(long jobId) {
                return kafkaRoutineLoadJob;
            }
        };

        new MockUp<KafkaUtil>() {
            @Mock
            public Map<Integer, Long> getLatestOffsets(String brokerList, String topic,
                                                       ImmutableMap<String, String> properties,
                                                       List<Integer> partitions,
                                                       ComputeResource computeResource) throws StarRocksException {
                Map<Integer, Long> offsets = Maps.newHashMap();
                offsets.put(0, 100L);
                offsets.put(1, 100L);
                return offsets;
            }
        };

        Map<Integer, Long> offset1 = Maps.newHashMap();
        offset1.put(0, 99L);
        KafkaTaskInfo kafkaTaskInfo1 = new KafkaTaskInfo(UUIDUtil.genUUID(),
                kafkaRoutineLoadJob,
                System.currentTimeMillis(),
                System.currentTimeMillis(),
                offset1,
                Config.routine_load_task_timeout_second * 1000);
        Assertions.assertEquals("kafka", kafkaTaskInfo1.dataSourceType());
        Assertions.assertTrue(kafkaTaskInfo1.readyToExecute());

        Map<Integer, Long> offset2 = Maps.newHashMap();
        offset2.put(0, 100L);
        KafkaTaskInfo kafkaTaskInfo2 = new KafkaTaskInfo(UUIDUtil.genUUID(),
                kafkaRoutineLoadJob,
                System.currentTimeMillis(),
                System.currentTimeMillis(),
                offset2,
                Config.routine_load_task_timeout_second * 1000);
        Assertions.assertFalse(kafkaTaskInfo2.readyToExecute());

        // consume offset > latest offset
        Map<Integer, Long> offset3 = Maps.newHashMap();
        offset3.put(0, 101L);
        KafkaTaskInfo kafkaTaskInfo3 = new KafkaTaskInfo(UUIDUtil.genUUID(),
                kafkaRoutineLoadJob,
                System.currentTimeMillis(),
                System.currentTimeMillis(),
                offset3,
                Config.routine_load_task_timeout_second * 1000);
        ExceptionChecker.expectThrowsWithMsg(RoutineLoadPauseException.class,
                "Consume offset: 101 is greater than the latest offset: 100 in kafka partition: 0. " +
                        "You can modify 'kafka_offsets' property through ALTER ROUTINE LOAD and RESUME the job",
                () -> kafkaTaskInfo3.readyToExecute());
    }

    @Test
    public void testCheckReadyToExecuteFast() {
        KafkaRoutineLoadJob kafkaRoutineLoadJob = new KafkaRoutineLoadJob();
        kafkaRoutineLoadJob.setPartitionOffset(0, 101);

        new MockUp<RoutineLoadMgr>() {
            @Mock
            public RoutineLoadJob getJob(long jobId) {
                return kafkaRoutineLoadJob;
            }
        };

        Map<Integer, Long> offset1 = Maps.newHashMap();
        offset1.put(0, 100L);
        KafkaTaskInfo kafkaTaskInfo = new KafkaTaskInfo(UUIDUtil.genUUID(),
                kafkaRoutineLoadJob,
                System.currentTimeMillis(),
                System.currentTimeMillis(),
                offset1,
                Config.routine_load_task_timeout_second * 1000);

        Assertions.assertTrue(kafkaTaskInfo.checkReadyToExecuteFast());
    }

    @Test
    public void testProgressKeepUp(@Injectable KafkaRoutineLoadJob kafkaRoutineLoadJob) throws Exception {
        new MockUp<RoutineLoadMgr>() {
            @Mock
            public RoutineLoadJob getJob(long jobId) {
                return kafkaRoutineLoadJob;
            }
        };

        new MockUp<KafkaUtil>() {
            @Mock
            public Map<Integer, Long> getLatestOffsets(String brokerList, String topic,
                                                       ImmutableMap<String, String> properties,
                                                       List<Integer> partitions,
                                                       ComputeResource computeResource) throws StarRocksException {
                Map<Integer, Long> offsets = Maps.newHashMap();
                offsets.put(0, 100L);
                offsets.put(1, 100L);
                return offsets;
            }
        };

        Map<Integer, Long> offset = Maps.newHashMap();
        offset.put(0, 99L);
        KafkaTaskInfo kafkaTaskInfo = new KafkaTaskInfo(UUIDUtil.genUUID(),
                kafkaRoutineLoadJob,
                System.currentTimeMillis(),
                System.currentTimeMillis(),
                offset,
                Config.routine_load_task_timeout_second * 1000);
        // call readyExecute to cache latestPartOffset
        kafkaTaskInfo.readyToExecute();

        KafkaProgress kafkaProgress = new KafkaProgress();
        kafkaProgress.addPartitionOffset(new Pair<>(0, 98L));
        kafkaProgress.addPartitionOffset(new Pair<>(1, 98L));
        Assertions.assertFalse(kafkaTaskInfo.isProgressKeepUp(kafkaProgress));

        kafkaProgress.modifyOffset(Lists.newArrayList(new Pair<>(0, 99L)));
        Assertions.assertFalse(kafkaTaskInfo.isProgressKeepUp(kafkaProgress));

        kafkaProgress.modifyOffset(Lists.newArrayList(new Pair<>(1, 99L)));
        Assertions.assertTrue(kafkaTaskInfo.isProgressKeepUp(kafkaProgress));
    }
}
