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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;

import org.junit.jupiter.api.Test;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;

class TestIcebergTableFormatTableType {
  @Test
  void copyOnWriteTablesAreAccepted() {
    assertDoesNotThrow(
        () -> IcebergTableFormat.requireCopyOnWrite(tableConfig(HoodieTableType.COPY_ON_WRITE)));
  }

  @Test
  void mergeOnReadTablesAreRejected() {
    // Log files never become Iceberg data files, so a deltacommit that writes only log files would
    // publish an empty snapshot and Iceberg readers would silently miss the updates.
    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                IcebergTableFormat.requireCopyOnWrite(tableConfig(HoodieTableType.MERGE_ON_READ)));
    assertTrue(exception.getMessage().contains("MERGE_ON_READ"), exception.getMessage());
    assertTrue(exception.getMessage().contains("test_table"), exception.getMessage());
  }

  @Test
  void mergeOnReadTablesAreRejectedWhenTheFormatIsInitialized() {
    // Hudi initializes the format with hoodie.properties on every meta client load, so the table is
    // refused before any instant exists.
    Properties properties = new Properties();
    properties.setProperty(HoodieTableConfig.NAME.key(), "test_table");
    properties.setProperty(HoodieTableConfig.TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
    assertThrows(
        UnsupportedOperationException.class, () -> new IcebergTableFormat().init(properties));
  }

  @Test
  void tableTypeDefaultsToCopyOnWrite() {
    assertDoesNotThrow(() -> new IcebergTableFormat().init(new Properties()));
  }

  private static HoodieTableConfig tableConfig(HoodieTableType tableType) {
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    tableConfig.setValue(HoodieTableConfig.NAME, "test_table");
    tableConfig.setValue(HoodieTableConfig.TYPE, tableType.name());
    return tableConfig;
  }
}
