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
 
package org.apache.xtable.conversion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.model.sync.SyncMode;

class TestConversionUtils {

  @Test
  void testNormalizeTargetPaths() {
    ConversionConfig config =
        ConversionConfig.builder()
            .sourceTable(
                SourceTable.builder()
                    .name("table_name")
                    .formatName(TableFormat.ICEBERG)
                    .basePath("/tmp/basePath")
                    .dataPath("/tmp/basePath/data")
                    .build())
            .syncMode(SyncMode.FULL)
            .targetTables(
                Arrays.asList(
                    TargetTable.builder()
                        .name("table_name")
                        .basePath("/tmp/basePath")
                        .formatName(TableFormat.DELTA)
                        .build(),
                    TargetTable.builder()
                        .name("table_name")
                        .basePath("/tmp/basePath")
                        .formatName(TableFormat.HUDI)
                        .build()))
            .build();
    ConversionConfig expectedNormalizedConfig =
        ConversionConfig.builder()
            .sourceTable(
                SourceTable.builder()
                    .name("table_name")
                    .formatName(TableFormat.ICEBERG)
                    .basePath("/tmp/basePath")
                    .dataPath("/tmp/basePath/data")
                    .build())
            .syncMode(SyncMode.FULL)
            .targetTables(
                Arrays.asList(
                    TargetTable.builder()
                        .name("table_name")
                        .basePath("/tmp/basePath/data")
                        .formatName(TableFormat.DELTA)
                        .build(),
                    TargetTable.builder()
                        .name("table_name")
                        .basePath("/tmp/basePath/data")
                        .formatName(TableFormat.HUDI)
                        .build()))
            .build();
    ConversionConfig actualConfig = ConversionUtils.normalizeTargetPaths(config);
    assertEquals(expectedNormalizedConfig, actualConfig);
  }

  @Test
  void testNormalizeTargetPathsNoOp() {
    ConversionConfig config =
        ConversionConfig.builder()
            .sourceTable(
                SourceTable.builder()
                    .name("table_name")
                    .formatName(TableFormat.HUDI)
                    .basePath("/tmp/basePath")
                    .build())
            .syncMode(SyncMode.FULL)
            .targetTables(
                Arrays.asList(
                    TargetTable.builder()
                        .name("table_name")
                        .basePath("/tmp/basePath")
                        .formatName(TableFormat.ICEBERG)
                        .build(),
                    TargetTable.builder()
                        .name("table_name")
                        .basePath("/tmp/basePath")
                        .formatName(TableFormat.DELTA)
                        .build()))
            .build();
    ConversionConfig actualConfig = ConversionUtils.normalizeTargetPaths(config);
    assertEquals(config, actualConfig);
  }
}
