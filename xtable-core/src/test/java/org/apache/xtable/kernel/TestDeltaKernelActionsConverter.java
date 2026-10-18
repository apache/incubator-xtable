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
 
package org.apache.xtable.kernel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.RemoveFile;

import org.apache.xtable.model.stat.FileStats;
import org.apache.xtable.model.storage.FileFormat;
import org.apache.xtable.model.storage.InternalDataFile;

class TestDeltaKernelActionsConverter {
  private static final String TABLE_BASE_PATH = "s3a://bucket/tab";

  private final DeltaKernelActionsConverter actionsConverter =
      DeltaKernelActionsConverter.getInstance();

  @ParameterizedTest
  @CsvSource({
    "part-0.parquet, s3a://bucket/tab/part-0.parquet",
    "s3://bucket/tab/part-0.parquet, s3://bucket/tab/part-0.parquet",
    "/tmp/other/part-0.parquet, /tmp/other/part-0.parquet"
  })
  void convertAddActionResolvesPath(String loggedPath, String expectedPath) {
    AddFile addFile = mock(AddFile.class);
    when(addFile.getPath()).thenReturn(loggedPath);
    DeltaKernelStatsExtractor statsExtractor = mock(DeltaKernelStatsExtractor.class);
    when(statsExtractor.getColumnStatsForFile(any(), any()))
        .thenReturn(FileStats.builder().columnStats(Collections.emptyList()).build());

    InternalDataFile dataFile =
        actionsConverter.convertAddActionToInternalDataFile(
            addFile,
            TABLE_BASE_PATH,
            FileFormat.APACHE_PARQUET,
            Collections.emptyList(),
            Collections.emptyList(),
            false,
            DeltaKernelPartitionExtractor.getInstance(),
            statsExtractor,
            Collections.emptyMap());

    assertEquals(expectedPath, dataFile.getPhysicalPath());
  }

  @ParameterizedTest
  @CsvSource({
    "part-0.parquet, s3a://bucket/tab/part-0.parquet",
    "s3://bucket/tab/part-0.parquet, s3://bucket/tab/part-0.parquet",
    "/tmp/other/part-0.parquet, /tmp/other/part-0.parquet"
  })
  void convertRemoveActionResolvesPath(String loggedPath, String expectedPath) {
    RemoveFile removeFile = mock(RemoveFile.class);
    when(removeFile.getPath()).thenReturn(loggedPath);

    InternalDataFile dataFile =
        actionsConverter.convertRemoveActionToInternalDataFile(
            removeFile,
            TABLE_BASE_PATH,
            FileFormat.APACHE_PARQUET,
            Collections.emptyList(),
            DeltaKernelPartitionExtractor.getInstance(),
            Collections.emptyMap());

    assertEquals(expectedPath, dataFile.getPhysicalPath());
  }
}
