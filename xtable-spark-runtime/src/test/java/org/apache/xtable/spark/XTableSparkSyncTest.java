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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

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
}
