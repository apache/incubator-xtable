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
 
package org.apache.xtable.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.xtable.model.sync.ErrorDetails;
import org.apache.xtable.model.sync.SyncResult;
import org.apache.xtable.model.sync.SyncStatusCode;

class XTableSparkSyncTest {

  @ParameterizedTest
  @CsvSource({
    "/warehouse/db/orders, orders",
    "/warehouse/db/orders/, orders",
    "/warehouse/db/orders///, orders",
    "orders, orders",
    "hdfs://ns/warehouse/db/orders, orders",
  })
  void basePathToTableNameDerivesLastSegment(String basePath, String expected) {
    assertEquals(expected, XTableSparkSync.basePathToTableName(basePath));
  }

  @ParameterizedTest
  @ValueSource(strings = {"/", "//", ""})
  void basePathToTableNameRejectsPathsWithNoUsableSegment(String basePath) {
    assertThrows(
        IllegalArgumentException.class, () -> XTableSparkSync.basePathToTableName(basePath));
  }

  @Test
  void basePathToTableNameRejectsNull() {
    assertThrows(IllegalArgumentException.class, () -> XTableSparkSync.basePathToTableName(null));
  }

  @ParameterizedTest
  @ValueSource(strings = {"HUDI", "ICEBERG", "DELTA", "PAIMON", "PARQUET"})
  void validateFormatAcceptsSupportedSourceFormats(String format) {
    XTableSparkSync.validateFormat(
        "sourceformat", format, XTableSparkSync.SUPPORTED_SOURCE_FORMATS);
  }

  @ParameterizedTest
  @ValueSource(strings = {"HUDI", "ICEBERG", "DELTA"})
  void validateFormatAcceptsSupportedTargetFormats(String format) {
    XTableSparkSync.validateFormat("targets", format, XTableSparkSync.SUPPORTED_TARGET_FORMATS);
  }

  @ParameterizedTest
  @ValueSource(strings = {"PAIMON", "PARQUET"})
  void validateFormatRejectsSourceOnlyFormatsAsTargets(String format) {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            XTableSparkSync.validateFormat(
                "targets", format, XTableSparkSync.SUPPORTED_TARGET_FORMATS));
  }

  @ParameterizedTest
  @ValueSource(strings = {"HOODIE", "hudi", "", "ICEBERG,DELTA"})
  void validateFormatRejectsUnsupportedFormats(String format) {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            XTableSparkSync.validateFormat(
                "sourceformat", format, XTableSparkSync.SUPPORTED_SOURCE_FORMATS));
  }

  @ParameterizedTest
  @ValueSource(strings = {"3.5.0", "3.5.9", "3.6.0", "4.0.0", "4.0.0-preview", "10.2.1"})
  void isSparkAtLeast35TrueFor35AndNewer(String version) {
    assertTrue(XTableSparkSync.isSparkAtLeast35(version));
  }

  @ParameterizedTest
  @ValueSource(strings = {"3.4.3", "3.4.0", "3.3.4", "2.4.8", "3.0.0"})
  void isSparkAtLeast35FalseForOlder(String version) {
    assertFalse(XTableSparkSync.isSparkAtLeast35(version));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "   ", "not-a-version", "x.y.z"})
  void isSparkAtLeast35FalseForNullOrUnparseable(String version) {
    assertFalse(XTableSparkSync.isSparkAtLeast35(version));
  }

  @Test
  void isSparkAtLeast35FalseForNull() {
    assertFalse(XTableSparkSync.isSparkAtLeast35(null));
  }

  @Test
  void syncAllCountsReturnedErrorsAndThrownFailuresAndContinuesBatch() {
    List<TableSyncSpec> specs =
        Arrays.asList(spec("ok"), spec("target_error"), spec("throws"), spec("ok_after"));
    List<String> synced = new ArrayList<>();
    int failures =
        XTableSparkSync.syncAll(
            specs,
            spec -> {
              synced.add(spec.getKey());
              Map<String, SyncResult> results = new HashMap<>();
              results.put("ICEBERG", syncResult(SyncStatusCode.SUCCESS));
              switch (spec.getKey()) {
                case "target_error":
                  results.put("DELTA", syncResult(SyncStatusCode.ERROR));
                  return results;
                case "throws":
                  throw new IllegalStateException("source unreadable");
                default:
                  results.put("DELTA", syncResult(SyncStatusCode.SUCCESS));
                  return results;
              }
            });
    assertEquals(2, failures);
    assertEquals(Arrays.asList("ok", "target_error", "throws", "ok_after"), synced);
  }

  private static TableSyncSpec spec(String key) {
    return TableSyncSpec.builder()
        .key(key)
        .basePath("/warehouse/" + key)
        .sourceFormat("HUDI")
        .targets(Arrays.asList("ICEBERG", "DELTA"))
        .build();
  }

  private static SyncResult syncResult(SyncStatusCode statusCode) {
    return SyncResult.builder()
        .tableFormatSyncStatus(
            SyncResult.SyncStatus.builder()
                .statusCode(statusCode)
                .errorDetails(
                    statusCode == SyncStatusCode.SUCCESS
                        ? null
                        : ErrorDetails.builder().errorMessage("commit conflict").build())
                .build())
        .build();
  }

  private static InputStream yaml(String content) {
    return new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void parseDatasetConfigReadsAndNormalizesFields() {
    XTableSparkSync.DatasetConfig config =
        XTableSparkSync.parseDatasetConfig(
            yaml(
                "sourceFormat: hudi\n"
                    + "targetFormats:\n"
                    + "  - iceberg\n"
                    + "  - DELTA\n"
                    + "datasets:\n"
                    + "  - tableBasePath: /data/orders\n"
                    + "    tableName: orders\n"
                    + "    partitionSpec: level:VALUE\n"
                    + "  - tableBasePath: /data/customers\n"
                    + "    namespace: db.sales\n"));

    assertEquals("HUDI", config.sourceFormat);
    assertEquals(2, config.targetFormats.size());
    assertEquals("ICEBERG", config.targetFormats.get(0));
    assertEquals("DELTA", config.targetFormats.get(1));
    assertEquals(2, config.datasets.size());
    assertEquals("/data/orders", config.datasets.get(0).tableBasePath);
    assertEquals("orders", config.datasets.get(0).tableName);
    assertEquals("level:VALUE", config.datasets.get(0).partitionSpec);
    assertEquals("/data/customers", config.datasets.get(1).tableBasePath);
    assertEquals("db.sales", config.datasets.get(1).namespace);
    assertNull(config.datasets.get(1).tableName);
  }

  @Test
  void parseDatasetConfigRejectsMissingSourceFormat() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            XTableSparkSync.parseDatasetConfig(
                yaml("targetFormats: [ICEBERG]\ndatasets:\n  - tableBasePath: /data/orders\n")));
  }

  @Test
  void parseDatasetConfigRejectsEmptyTargetFormats() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            XTableSparkSync.parseDatasetConfig(
                yaml("sourceFormat: HUDI\ntargetFormats: []\ndatasets:\n  - tableBasePath: /d\n")));
  }

  @Test
  void parseDatasetConfigRejectsInvalidTargetFormat() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            XTableSparkSync.parseDatasetConfig(
                yaml(
                    "sourceFormat: HUDI\ntargetFormats: [PAIMON]\ndatasets:\n  - tableBasePath: /d\n")));
  }

  @Test
  void parseDatasetConfigRejectsMissingDatasets() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            XTableSparkSync.parseDatasetConfig(
                yaml("sourceFormat: HUDI\ntargetFormats: [DELTA]\n")));
  }

  @Test
  void parseDatasetConfigRejectsDatasetWithoutBasePath() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            XTableSparkSync.parseDatasetConfig(
                yaml(
                    "sourceFormat: HUDI\ntargetFormats: [DELTA]\ndatasets:\n  - tableName: orders\n")));
  }
}
