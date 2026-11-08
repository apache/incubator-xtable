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
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class TestExternalTable {
  @Test
  void sanitizePath() {
    ExternalTable tooManySlashes =
        new ExternalTable("name", "hudi", "s3://bucket//path", null, null, null);
    assertEquals("s3://bucket/path", tooManySlashes.getBasePath());

    ExternalTable localFilePath =
        new ExternalTable("name", "hudi", "/local/data//path", null, null, null);
    assertEquals("/local/data/path", localFilePath.getBasePath());

    ExternalTable properLocalFilePath =
        new ExternalTable("name", "hudi", "file:///local/data//path", null, null, null);
    assertEquals("file:///local/data/path", properLocalFilePath.getBasePath());

    ExternalTable windowsLocalPath =
        new ExternalTable("name", "hudi", "C:/local/data//path", null, null, null);
    assertEquals("file:///C:/local/data/path", windowsLocalPath.getBasePath());

    ExternalTable windowsBackslashPath =
        new ExternalTable("name", "hudi", "C:\\local\\data\\\\path", null, null, null);
    assertEquals("file:///C:/local/data/path", windowsBackslashPath.getBasePath());
  }

  @Test
  void sourceAndTargetTablesUseSharedPathSanitization() {
    SourceTable sourceTable =
        SourceTable.builder()
            .name("source")
            .formatName("delta")
            .basePath("/local/source//path")
            .dataPath("file:///local/source//data")
            .build();
    TargetTable targetTable =
        TargetTable.builder()
            .name("target")
            .formatName("delta")
            .basePath("/local/target//path")
            .build();
    SourceTable windowsSourceTable =
        SourceTable.builder()
            .name("windows-source")
            .formatName("delta")
            .basePath("C:/local/source//path")
            .build();
    TargetTable windowsTargetTable =
        TargetTable.builder()
            .name("windows-target")
            .formatName("delta")
            .basePath("C:\\local\\target\\\\path")
            .build();

    assertEquals("/local/source/path", sourceTable.getBasePath());
    assertEquals("file:///local/source/data", sourceTable.getDataPath());
    assertEquals("/local/target/path", targetTable.getBasePath());
    assertEquals("file:///C:/local/source/path", windowsSourceTable.getBasePath());
    assertEquals("file:///C:/local/target/path", windowsTargetTable.getBasePath());
  }

  @Test
  void errorIfRequiredArgsNotSet() {
    assertThrows(
        NullPointerException.class,
        () -> new ExternalTable("name", "hudi", null, null, null, null));

    assertThrows(
        NullPointerException.class,
        () -> new ExternalTable("name", null, "file://bucket/path", null, null, null));

    assertThrows(
        NullPointerException.class,
        () -> new ExternalTable(null, "hudi", "file://bucket/path", null, null, null));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            new ExternalTable(
                "name", "hudi", "relative/table/path", null, null, null));
  }
}
