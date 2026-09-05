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
 
package org.apache.xtable.glue;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

public class TestGlueCatalogConfig {
  private static final String GLUE_CATALOG_ID_KEY = "externalCatalog.glue.catalogId";
  private static final String GLUE_CATALOG_ID_VALUE = "aws-accountId";
  private static final String GLUE_CATALOG_REGION_KEY = "externalCatalog.glue.region";
  private static final String GLUE_CATALOG_REGION_VALUE = "aws-region";
  private static final String GLUE_CATALOG_CREDENTIAL_PROVIDER_CLASS_KEY =
      "externalCatalog.glue.credentialsProviderClass";
  private static final String GLUE_CATALOG_CREDENTIAL_PROVIDER_CLASS_VALUE =
      "credentialsProviderClass";
  private static final String GLUE_CATALOG_CREDENTIAL_PROVIDER_PROP_ACCESS_KEY =
      "externalCatalog.glue.credentials.provider.accessKey";
  private static final String GLUE_CATALOG_CREDENTIAL_PROVIDER_PROP_ACCESS_KEY_VALUE = "accessKey";
  private static final String GLUE_CATALOG_CREDENTIAL_PROVIDER_PROP_SECRET_ACCESS_KEY =
      "externalCatalog.glue.credentials.provider.secretAccessKey";
  private static final String GLUE_CATALOG_CREDENTIAL_PROVIDER_PROP_SECRET_ACCESS_KEY_VALUE =
      "secretAccessKey";

  @Test
  void testGetGlueCatalogConfig_withNoPropertiesSet() {
    Map<String, String> props = Collections.emptyMap();
    GlueCatalogConfig catalogConfig = GlueCatalogConfig.of(props);
    assertNull(catalogConfig.getCatalogId());
    assertNull(catalogConfig.getRegion());
    assertNull(catalogConfig.getClientCredentialsProviderClass());
    assertNotNull(catalogConfig.getClientCredentialsProviderConfigs());
    assertEquals(0, catalogConfig.getClientCredentialsProviderConfigs().size());
  }

  @Test
  void testGetGlueCatalogConfig_withMissingProperties() {
    Map<String, String> props =
        createProps(
            GLUE_CATALOG_ID_KEY,
            GLUE_CATALOG_ID_VALUE,
            GLUE_CATALOG_REGION_KEY,
            GLUE_CATALOG_REGION_VALUE);
    GlueCatalogConfig catalogConfig = GlueCatalogConfig.of(props);
    assertEquals(GLUE_CATALOG_ID_VALUE, catalogConfig.getCatalogId());
    assertEquals(GLUE_CATALOG_REGION_VALUE, catalogConfig.getRegion());
    assertNull(catalogConfig.getClientCredentialsProviderClass());
    assertNotNull(catalogConfig.getClientCredentialsProviderConfigs());
    assertEquals(0, catalogConfig.getClientCredentialsProviderConfigs().size());
  }

  @Test
  void testGetGlueCatalogConfig_withUnknownProperty() {
    Map<String, String> props =
        createProps(
            GLUE_CATALOG_ID_KEY,
            GLUE_CATALOG_ID_VALUE,
            GLUE_CATALOG_REGION_KEY,
            GLUE_CATALOG_REGION_VALUE,
            "externalCatalog.glue.unknownProperty",
            "unknown-property-value");
    GlueCatalogConfig catalogConfig = assertDoesNotThrow(() -> GlueCatalogConfig.of(props));
    assertEquals(GLUE_CATALOG_ID_VALUE, catalogConfig.getCatalogId());
    assertEquals(GLUE_CATALOG_REGION_VALUE, catalogConfig.getRegion());
    assertNull(catalogConfig.getClientCredentialsProviderClass());
    assertNotNull(catalogConfig.getClientCredentialsProviderConfigs());
    assertEquals(0, catalogConfig.getClientCredentialsProviderConfigs().size());
  }

  @Test
  void testGetGlueCatalogConfig_withAllPropertiesSet() {
    Map<String, String> props =
        createProps(
            GLUE_CATALOG_ID_KEY,
            GLUE_CATALOG_ID_VALUE,
            GLUE_CATALOG_REGION_KEY,
            GLUE_CATALOG_REGION_VALUE,
            GLUE_CATALOG_CREDENTIAL_PROVIDER_CLASS_KEY,
            GLUE_CATALOG_CREDENTIAL_PROVIDER_CLASS_VALUE,
            GLUE_CATALOG_CREDENTIAL_PROVIDER_PROP_ACCESS_KEY,
            GLUE_CATALOG_CREDENTIAL_PROVIDER_PROP_ACCESS_KEY_VALUE,
            GLUE_CATALOG_CREDENTIAL_PROVIDER_PROP_SECRET_ACCESS_KEY,
            GLUE_CATALOG_CREDENTIAL_PROVIDER_PROP_SECRET_ACCESS_KEY_VALUE);
    GlueCatalogConfig catalogConfig = GlueCatalogConfig.of(props);
    assertEquals(GLUE_CATALOG_ID_VALUE, catalogConfig.getCatalogId());
    assertEquals(GLUE_CATALOG_REGION_VALUE, catalogConfig.getRegion());
    assertEquals(
        GLUE_CATALOG_CREDENTIAL_PROVIDER_CLASS_VALUE,
        catalogConfig.getClientCredentialsProviderClass());
    assertNotNull(catalogConfig.getClientCredentialsProviderConfigs());
    assertEquals(2, catalogConfig.getClientCredentialsProviderConfigs().size());
  }

  private Map<String, String> createProps(String... keyValues) {
    Map<String, String> props = new HashMap<>();
    for (int i = 0; i < keyValues.length; i += 2) {
      props.put(keyValues[i], keyValues[i + 1]);
    }
    return props;
  }
}
