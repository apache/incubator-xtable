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

public class TestDataFilesDiff {
  @Test
  void testFrom() {
    InternalDataFile sourceFile1 =
        InternalDataFile.builder().physicalPath("file://new_source_file1.parquet").build();
    InternalDataFile sourceFile2 =
        InternalDataFile.builder().physicalPath("file://new_source_file2.parquet").build();
    InternalDataFile targetFile1 =
        InternalDataFile.builder().physicalPath("file://already_in_target1.parquet").build();
    InternalDataFile targetFile2 =
        InternalDataFile.builder().physicalPath("file://already_in_target2.parquet").build();
    InternalDataFile sourceFileInTargetAlready =
        InternalDataFile.builder().physicalPath("file://already_in_target3.parquet").build();
    DataFilesDiff actual =
        DataFilesDiff.from(
            Arrays.asList(sourceFile1, sourceFile2, sourceFileInTargetAlready),
            Arrays.asList(targetFile1, targetFile2, sourceFileInTargetAlready));

    DataFilesDiff expected =
        DataFilesDiff.builder()
            .filesAdded(Arrays.asList(sourceFile1, sourceFile2))
            .filesRemoved(Arrays.asList(targetFile1, targetFile2))
            .build();
    assertEquals(expected, actual);
  }
}
