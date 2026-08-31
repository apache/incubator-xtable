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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;

import org.junit.jupiter.api.Test;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableVersion;

/**
 * Guards the table-format override that {@link TestAbstractHudiTable} applies. Every Hudi test in
 * the repository shares that harness, so the override has to stay inert unless a module explicitly
 * asks for a pluggable format.
 */
class TestHudiTableFormatOverrides {

  @Test
  void emptyForTheNativeFormat() {
    assertTrue(
        TestAbstractHudiTable.tableFormatOverrides(null).isEmpty(),
        "a module that does not ask for a pluggable format must get no overrides");
  }

  @Test
  void suppliesFormatVersionAndMetadataSettingForAPluggableFormat() {
    Properties overrides = TestAbstractHudiTable.tableFormatOverrides("ICEBERG");
    assertEquals("ICEBERG", overrides.getProperty(HoodieTableConfig.TABLE_FORMAT.key()));
    assertEquals(
        String.valueOf(HoodieTableVersion.EIGHT.versionCode()),
        overrides.getProperty(HoodieTableConfig.VERSION.key()),
        "a pluggable format needs the v2 timeline layout, which table version 6 does not have");
    assertEquals("false", overrides.getProperty(HoodieMetadataConfig.ENABLE.key()));
  }
}
