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
 
package io.onetable.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class TestTargetTable {
  @Test
  void sanitizePath() {
    TargetTable tooManySlashes =
        TargetTable.builder().basePath("s3://bucket//path").name("name").formatName("hudi").build();
    assertEquals("s3://bucket/path", tooManySlashes.getMetadataPath());

    TargetTable localFilePath =
        TargetTable.builder().basePath("/local/data//path").name("name").formatName("hudi").build();
    assertEquals("file:///local/data/path", localFilePath.getMetadataPath());

    TargetTable properLocalFilePath =
        TargetTable.builder()
            .basePath("file:///local/data//path")
            .name("name")
            .formatName("hudi")
            .build();
    assertEquals("file:///local/data/path", properLocalFilePath.getMetadataPath());
  }

  @Test
  void defaultValueSet() {
    TargetTable table =
        TargetTable.builder()
            .basePath("file://bucket/path")
            .name("name")
            .formatName("hudi")
            .build();

    assertEquals(24 * 7, table.getMetadataRetentionInHours());
    assertNull(table.getNamespace());
    assertNull(table.getCatalogConfig());
  }

  @Test
  void errorIfRequiredArgsNotSet() {
    assertThrows(NullPointerException.class, () -> SourceTable.builder().name("name").build());

    assertThrows(
        NullPointerException.class,
        () -> SourceTable.builder().basePath("file://bucket/path").build());

    assertThrows(
        NullPointerException.class,
        () -> SourceTable.builder().basePath("file://bucket/path").name("name").build());
  }
}
