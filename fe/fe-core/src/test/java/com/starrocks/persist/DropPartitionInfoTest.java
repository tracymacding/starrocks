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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/persist/DropPartitionInfoTest.java

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

package com.starrocks.persist;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

public class DropPartitionInfoTest {
    @Test
    public void testSerialization() throws Exception {
        // 1. Write objects to file
        File file = new File("./dropPartitionInfo");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));

        DropPartitionInfo info1 = new DropPartitionInfo(1L, 2L, "test_partition", false, true);
        info1.write(dos);

        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));

        DropPartitionInfo rInfo1 = DropPartitionInfo.read(dis);

        Assertions.assertEquals(Long.valueOf(1L), rInfo1.getDbId());
        Assertions.assertEquals(Long.valueOf(2L), rInfo1.getTableId());
        Assertions.assertEquals("test_partition", rInfo1.getPartitionName());
        Assertions.assertFalse(rInfo1.isTempPartition());
        Assertions.assertTrue(rInfo1.isForceDrop());

        Assertions.assertTrue(rInfo1.equals(info1));
        Assertions.assertFalse(rInfo1.equals(this));
        Assertions.assertFalse(info1.equals(new DropPartitionInfo(-1L, 2L, "test_partition", false, true)));
        Assertions.assertFalse(info1.equals(new DropPartitionInfo(1L, -2L, "test_partition", false, true)));
        Assertions.assertFalse(info1.equals(new DropPartitionInfo(1L, 2L, "test_partition1", false, true)));
        Assertions.assertFalse(info1.equals(new DropPartitionInfo(1L, 2L, "test_partition", true, true)));
        Assertions.assertFalse(info1.equals(new DropPartitionInfo(1L, 2L, "test_partition", false, false)));
        Assertions.assertTrue(info1.equals(new DropPartitionInfo(1L, 2L, "test_partition", false, true)));

        // 3. delete files
        dis.close();
        file.delete();
    }
}
