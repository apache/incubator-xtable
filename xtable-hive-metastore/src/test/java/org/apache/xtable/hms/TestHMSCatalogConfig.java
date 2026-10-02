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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

public class TestHMSCatalogConfig {

  private static final String HMS_CATALOG_SERVER_URL_KEY = "externalCatalog.hms.serverUrl";
  private static final String HMS_CATALOG_SERVER_URL_VALUE = "thrift://localhost:9083";

  @Test
  void testGetHmsCatalogConfig_withNoPropertiesSet() {
    Map<String, String> props = new HashMap<>();
    HMSCatalogConfig catalogConfig = HMSCatalogConfig.of(props);
    assertNull(catalogConfig.getServerUrl());
  }

  @Test
  void testGetHmsCatalogConfig_withUnknownProperty() {
    Map<String, String> props =
        createProps("externalCatalog.glue.unknownProperty", "unknown-property-value");
    assertDoesNotThrow(() -> HMSCatalogConfig.of(props));
  }

  @Test
  void testGetHmsCatalogConfig() {
    Map<String, String> props =
        createProps(HMS_CATALOG_SERVER_URL_KEY, HMS_CATALOG_SERVER_URL_VALUE);
    HMSCatalogConfig catalogConfig = HMSCatalogConfig.of(props);
    assertEquals(HMS_CATALOG_SERVER_URL_VALUE, catalogConfig.getServerUrl());
  }

  private Map<String, String> createProps(String... keyValues) {
    Map<String, String> props = new HashMap<>();
    for (int i = 0; i < keyValues.length; i += 2) {
      props.put(keyValues[i], keyValues[i + 1]);
    }
    return props;
  }
}
