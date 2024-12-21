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
 
package org.apache.xtable.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;

import org.junit.jupiter.api.Test;

import org.apache.iceberg.TableProperties;

import org.apache.xtable.model.storage.TableFormat;

public class TestTableFormatUtils {

  @Test
  void testGetTableDataLocation_Hudi() {
    // For Hudi, data location should always be tableLocation
    String tableLocation = "base-path";
    assertEquals(
        tableLocation,
        TableFormatUtils.getTableDataLocation(
            TableFormat.HUDI, tableLocation, Collections.emptyMap()));
    assertEquals(
        tableLocation,
        TableFormatUtils.getTableDataLocation(
            TableFormat.HUDI,
            tableLocation,
            Collections.singletonMap(TableProperties.WRITE_DATA_LOCATION, "base-path/data")));
  }

  @Test
  void testGetTableDataLocation_Iceberg() {
    // For Iceberg, data location will be WRITE_DATA_LOCATION / OBJECT_STORE_PATH param or
    // tableLocation/data
    String tableLocation = "base-path";

    // no params is set
    assertEquals(
        tableLocation + "/data",
        TableFormatUtils.getTableDataLocation(
            TableFormat.ICEBERG, tableLocation, Collections.emptyMap()));

    // WRITE_DATA_LOCATION param is set
    String writeDataPath = "base-path/iceberg";
    assertEquals(
        writeDataPath,
        TableFormatUtils.getTableDataLocation(
            TableFormat.ICEBERG,
            tableLocation,
            Collections.singletonMap(TableProperties.WRITE_DATA_LOCATION, writeDataPath)));

    // OBJECT_STORE_PATH param is set
    String objectStorePath = "base-path/iceberg";
    assertEquals(
        objectStorePath,
        TableFormatUtils.getTableDataLocation(
            TableFormat.ICEBERG,
            tableLocation,
            Collections.singletonMap(TableProperties.OBJECT_STORE_PATH, objectStorePath)));
  }
}
