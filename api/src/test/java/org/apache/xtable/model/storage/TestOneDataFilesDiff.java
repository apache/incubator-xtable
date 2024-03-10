/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package org.apache.xtable.model.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

public class TestOneDataFilesDiff {
  @Test
  void testFrom() {
    OneDataFile sourceFile1 =
        OneDataFile.builder().physicalPath("file://new_source_file1.parquet").build();
    OneDataFile sourceFile2 =
        OneDataFile.builder().physicalPath("file://new_source_file2.parquet").build();
    OneDataFile targetFile1 =
        OneDataFile.builder().physicalPath("file://already_in_target1.parquet").build();
    OneDataFile targetFile2 =
        OneDataFile.builder().physicalPath("file://already_in_target2.parquet").build();
    OneDataFile sourceFileInTargetAlready =
        OneDataFile.builder().physicalPath("file://already_in_target3.parquet").build();
    OneDataFilesDiff actual =
        OneDataFilesDiff.from(
            Arrays.asList(sourceFile1, sourceFile2, sourceFileInTargetAlready),
            Arrays.asList(targetFile1, targetFile2, sourceFileInTargetAlready));

    OneDataFilesDiff expected =
        OneDataFilesDiff.builder()
            .filesAdded(Arrays.asList(sourceFile1, sourceFile2))
            .filesRemoved(Arrays.asList(targetFile1, targetFile2))
            .build();
    assertEquals(expected, actual);
  }
}
