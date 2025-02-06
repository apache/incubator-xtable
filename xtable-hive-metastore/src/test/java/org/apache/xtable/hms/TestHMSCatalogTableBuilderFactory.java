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
 
package org.apache.xtable.hms;

import static org.apache.xtable.hms.HMSCatalogSyncClientTestBase.FIELD_SCHEMA;
import static org.apache.xtable.hms.HMSCatalogSyncClientTestBase.TEST_CATALOG_TABLE_IDENTIFIER;
import static org.apache.xtable.hms.HMSCatalogSyncClientTestBase.TEST_HMS_DATABASE;
import static org.apache.xtable.hms.HMSCatalogSyncClientTestBase.TEST_HMS_TABLE;
import static org.apache.xtable.hms.table.TestIcebergHMSCatalogTableBuilder.getTestHmsTableParameters;
import static org.apache.xtable.hms.table.TestIcebergHMSCatalogTableBuilder.getTestHmsTableStorageDescriptor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mockStatic;

import java.time.Instant;

import lombok.SneakyThrows;

import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class TestHMSCatalogTableBuilderFactory {

  @SneakyThrows
  @Test
  void testNewHmsTable() {
    Instant createdTime = Instant.now();
    try (MockedStatic<Instant> mockZonedDateTime = mockStatic(Instant.class)) {
      mockZonedDateTime.when(Instant::now).thenReturn(createdTime);
      Table expected = new Table();
      expected.setDbName(TEST_HMS_DATABASE);
      expected.setTableName(TEST_HMS_TABLE);
      expected.setOwner(UserGroupInformation.getCurrentUser().getShortUserName());
      expected.setCreateTime((int) createdTime.getEpochSecond());
      expected.setSd(getTestHmsTableStorageDescriptor(FIELD_SCHEMA));
      expected.setTableType("EXTERNAL_TABLE");
      expected.setParameters(getTestHmsTableParameters());

      assertEquals(
          expected,
          HMSCatalogTableBuilderFactory.newHmsTable(
              TEST_CATALOG_TABLE_IDENTIFIER,
              getTestHmsTableStorageDescriptor(FIELD_SCHEMA),
              getTestHmsTableParameters()));
    }
  }
}
