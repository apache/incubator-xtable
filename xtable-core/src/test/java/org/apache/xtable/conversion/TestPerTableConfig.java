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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;

import org.junit.jupiter.api.Test;

import org.apache.xtable.hudi.HudiSourceConfigImpl;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.model.sync.SyncMode;

class TestPerTableConfig {

  @Test
  void sanitizePath() {
    PerTableConfig tooManySlashes =
        PerTableConfigImpl.builder()
            .tableBasePath("s3://bucket//path")
            .tableName("name")
            .targetTableFormats(Collections.singletonList(TableFormat.ICEBERG))
            .build();
    assertEquals("s3://bucket/path", tooManySlashes.getTableBasePath());

    PerTableConfig localFilePath =
        PerTableConfigImpl.builder()
            .tableBasePath("/local/data//path")
            .tableName("name")
            .targetTableFormats(Collections.singletonList(TableFormat.ICEBERG))
            .build();
    assertEquals("file:///local/data/path", localFilePath.getTableBasePath());

    PerTableConfig properLocalFilePath =
        PerTableConfigImpl.builder()
            .tableBasePath("file:///local/data//path")
            .tableName("name")
            .targetTableFormats(Collections.singletonList(TableFormat.ICEBERG))
            .build();
    assertEquals("file:///local/data/path", properLocalFilePath.getTableBasePath());
  }

  @Test
  void defaultValueSet() {
    PerTableConfig perTableConfig =
        PerTableConfigImpl.builder()
            .tableBasePath("file://bucket/path")
            .tableName("name")
            .targetTableFormats(Collections.singletonList(TableFormat.ICEBERG))
            .build();

    assertEquals(24 * 7, perTableConfig.getTargetMetadataRetentionInHours());
    assertEquals(SyncMode.INCREMENTAL, perTableConfig.getSyncMode());
    assertEquals(HudiSourceConfigImpl.builder().build(), perTableConfig.getHudiSourceConfig());
    assertNull(perTableConfig.getNamespace());
    assertNull(perTableConfig.getIcebergCatalogConfig());
  }

  @Test
  void errorIfRequiredArgsNotSet() {
    assertThrows(
        NullPointerException.class,
        () ->
            PerTableConfigImpl.builder()
                .tableName("name")
                .targetTableFormats(Collections.singletonList(TableFormat.ICEBERG))
                .build());

    assertThrows(
        NullPointerException.class,
        () ->
            PerTableConfigImpl.builder()
                .tableBasePath("file://bucket/path")
                .targetTableFormats(Collections.singletonList(TableFormat.ICEBERG))
                .build());

    assertThrows(
        NullPointerException.class,
        () ->
            PerTableConfigImpl.builder()
                .tableBasePath("file://bucket/path")
                .tableName("name")
                .build());
  }

  @Test
  void errorIfNoTargetsSet() {
    Exception thrownException =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                PerTableConfigImpl.builder()
                    .tableName("name")
                    .tableBasePath("file://bucket/path")
                    .targetTableFormats(Collections.emptyList())
                    .build());
    assertEquals("Please provide at-least one format to sync", thrownException.getMessage());
  }
}
