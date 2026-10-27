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
 
package org.apache.xtable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorType;

/**
 * The Iceberg table format maps Hudi partition fields to identity partitioning only, and rejects
 * tables whose partition path is derived from a formatted timestamp.
 */
class TestIcebergTableFormatPartitioning {

  @Test
  void unpartitionedTableHasNoPartitionSpec() {
    HoodieTableConfig tableConfig = tableConfig(KeyGeneratorType.NON_PARTITION, null);
    assertNull(IcebergTableFormat.partitionFieldSpec(tableConfig));
  }

  @Test
  void simpleAndComplexKeyGeneratorFieldsMapToIdentityPartitions() {
    assertEquals(
        "level:VALUE",
        IcebergTableFormat.partitionFieldSpec(tableConfig(KeyGeneratorType.SIMPLE, "level")));
    assertEquals(
        "level:VALUE,severity:VALUE",
        IcebergTableFormat.partitionFieldSpec(
            tableConfig(KeyGeneratorType.COMPLEX, "level,severity")));
  }

  @Test
  void customKeyGeneratorWithSimpleFieldsMapsToIdentityPartitions() {
    // Table version 8 stores the custom key generator's partition type alongside each field.
    assertEquals(
        "level:VALUE,severity:VALUE",
        IcebergTableFormat.partitionFieldSpec(
            tableConfig(KeyGeneratorType.CUSTOM, "level:simple,severity:SIMPLE")));
  }

  @Test
  void timestampBasedKeyGeneratorIsRejected() {
    HoodieTableConfig tableConfig = tableConfig(KeyGeneratorType.TIMESTAMP, "ts");
    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class,
            () -> IcebergTableFormat.partitionFieldSpec(tableConfig));
    assertTrue(exception.getMessage().contains(KeyGeneratorType.TIMESTAMP.getClassName()));
  }

  @Test
  void customKeyGeneratorWithTimestampFieldIsRejected() {
    HoodieTableConfig tableConfig =
        tableConfig(KeyGeneratorType.CUSTOM_AVRO, "level:simple,ts:timestamp");
    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class,
            () -> IcebergTableFormat.partitionFieldSpec(tableConfig));
    assertTrue(exception.getMessage().contains("ts"), exception.getMessage());
  }

  private static HoodieTableConfig tableConfig(
      KeyGeneratorType keyGeneratorType, String partitionFields) {
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    tableConfig.setValue(HoodieTableConfig.NAME, "test_table");
    tableConfig.setValue(
        HoodieTableConfig.KEY_GENERATOR_CLASS_NAME, keyGeneratorType.getClassName());
    if (partitionFields != null) {
      tableConfig.setValue(HoodieTableConfig.PARTITION_FIELDS, partitionFields);
    }
    return tableConfig;
  }
}
