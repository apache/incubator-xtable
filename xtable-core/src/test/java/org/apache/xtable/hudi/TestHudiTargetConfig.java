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
 
package org.apache.xtable.hudi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import org.apache.hudi.common.table.HoodieTableVersion;

public class TestHudiTargetConfig {

  @Test
  void defaultsToTableVersionSixAndJavaEngine() {
    HudiTargetConfig config = HudiTargetConfig.fromProperties(new Properties());
    assertEquals(HoodieTableVersion.SIX, config.getTableVersion());
    assertFalse(config.isSparkEngine());
    assertEquals(Optional.empty(), config.getSecondaryIndexColumn());
    assertEquals(Optional.empty(), config.getRecordIndexMinFileGroupCount());
    assertEquals(Optional.empty(), config.getRecordIndexMaxFileGroupCount());
    assertEquals(Optional.empty(), config.getSecondaryIndexParallelism());
    assertEquals(config, HudiTargetConfig.fromProperties(null));
  }

  @Test
  void honoursConfiguredVersion() {
    Properties sixProps = new Properties();
    sixProps.setProperty(HudiTargetConfig.HUDI_TABLE_VERSION, "6");
    assertEquals(
        HoodieTableVersion.SIX, HudiTargetConfig.fromProperties(sixProps).getTableVersion());

    Properties nineProps = new Properties();
    nineProps.setProperty(HudiTargetConfig.HUDI_TABLE_VERSION, "9");
    assertEquals(
        HoodieTableVersion.NINE, HudiTargetConfig.fromProperties(nineProps).getTableVersion());
  }

  @Test
  void rejectsUnsupportedVersion() {
    Properties props = new Properties();
    props.setProperty(HudiTargetConfig.HUDI_TABLE_VERSION, "8");
    assertThrows(IllegalArgumentException.class, () -> HudiTargetConfig.fromProperties(props));
  }

  @ParameterizedTest
  @CsvSource({"java, false", "JAVA, false", "spark, true", "Spark, true"})
  void parsesExecutionEngine(String engine, boolean expectSparkEngine) {
    Properties props = new Properties();
    props.setProperty(HudiTargetConfig.EXECUTION_ENGINE, engine);
    assertEquals(expectSparkEngine, HudiTargetConfig.fromProperties(props).isSparkEngine());
  }

  @Test
  void rejectsUnsupportedExecutionEngine() {
    Properties props = new Properties();
    props.setProperty(HudiTargetConfig.EXECUTION_ENGINE, "flink");
    assertThrows(IllegalArgumentException.class, () -> HudiTargetConfig.fromProperties(props));
  }

  @Test
  void parsesSecondaryIndexSettings() {
    Properties props = new Properties();
    props.setProperty(HudiTargetConfig.HUDI_TABLE_VERSION, "9");
    props.setProperty(HudiTargetConfig.SECONDARY_INDEX_COLUMN, " id ");
    props.setProperty(HudiTargetConfig.RECORD_INDEX_MIN_FILEGROUP_COUNT, "2");
    props.setProperty(HudiTargetConfig.RECORD_INDEX_MAX_FILEGROUP_COUNT, "4");
    props.setProperty(HudiTargetConfig.SECONDARY_INDEX_PARALLELISM, "8");
    HudiTargetConfig config = HudiTargetConfig.fromProperties(props);
    assertEquals(Optional.of("id"), config.getSecondaryIndexColumn());
    assertEquals(Optional.of(2), config.getRecordIndexMinFileGroupCount());
    assertEquals(Optional.of(4), config.getRecordIndexMaxFileGroupCount());
    assertEquals(Optional.of(8), config.getSecondaryIndexParallelism());
  }

  @Test
  void ignoresBlankSecondaryIndexColumn() {
    Properties props = new Properties();
    props.setProperty(HudiTargetConfig.SECONDARY_INDEX_COLUMN, "  ");
    assertFalse(HudiTargetConfig.fromProperties(props).getSecondaryIndexColumn().isPresent());
  }

  @Test
  void secondaryIndexRequiresTableVersionNine() {
    Properties props = new Properties();
    props.setProperty(HudiTargetConfig.SECONDARY_INDEX_COLUMN, "id");
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> HudiTargetConfig.fromProperties(props));
    assertTrue(exception.getMessage().contains(HudiTargetConfig.HUDI_TABLE_VERSION));
  }

  @ParameterizedTest
  @CsvSource({
    // only one bound of the record index file group count
    "xtable.hudi.target.metadata.record.index.min.filegroup.count, 2",
    "xtable.hudi.target.metadata.record.index.max.filegroup.count, 2",
    // non positive values
    "xtable.hudi.target.metadata.index.secondary.parallelism, 0",
    "xtable.hudi.target.metadata.index.secondary.parallelism, -1",
  })
  void rejectsInvalidIndexSettings(String key, String value) {
    Properties props = new Properties();
    props.setProperty(key, value);
    assertThrows(IllegalArgumentException.class, () -> HudiTargetConfig.fromProperties(props));
  }
}
