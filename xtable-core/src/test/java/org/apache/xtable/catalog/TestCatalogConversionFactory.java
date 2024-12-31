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

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import org.apache.xtable.conversion.ExternalCatalogConfig;
import org.apache.xtable.conversion.TargetCatalogConfig;
import org.apache.xtable.model.catalog.ThreePartHierarchicalTableIdentifier;
import org.apache.xtable.spi.extractor.CatalogConversionSource;
import org.apache.xtable.spi.sync.CatalogSyncClient;
import org.apache.xtable.testutil.ITTestUtils;
import org.apache.xtable.testutil.ITTestUtils.TestCatalogConversionSourceImpl;
import org.apache.xtable.testutil.ITTestUtils.TestCatalogSyncImpl;

class TestCatalogConversionFactory {

  @Test
  void createCatalogConversionSource() {
    ExternalCatalogConfig sourceCatalog =
        ExternalCatalogConfig.builder()
            .catalogId("catalogId")
            .catalogConversionSourceImpl(TestCatalogConversionSourceImpl.class.getName())
            .catalogProperties(Collections.emptyMap())
            .build();
    CatalogConversionSource catalogConversionSource =
        CatalogConversionFactory.createCatalogConversionSource(sourceCatalog, new Configuration());
    assertEquals(
        catalogConversionSource.getClass().getName(),
        TestCatalogConversionSourceImpl.class.getName());
  }

  @Test
  void createCatalogConversionSourceForCatalogType() {
    ExternalCatalogConfig sourceCatalog =
        ExternalCatalogConfig.builder()
            .catalogId("catalogId")
            .catalogType(ITTestUtils.TEST_CATALOG_TYPE)
            .catalogProperties(Collections.emptyMap())
            .build();
    CatalogConversionSource catalogConversionSource =
        CatalogConversionFactory.createCatalogConversionSource(sourceCatalog, new Configuration());
    assertEquals(
        catalogConversionSource.getClass().getName(),
        TestCatalogConversionSourceImpl.class.getName());
  }

  @Test
  void createCatalogSyncClient() {
    TargetCatalogConfig targetCatalogConfig =
        TargetCatalogConfig.builder()
            .catalogConfig(
                ExternalCatalogConfig.builder()
                    .catalogId("catalogId")
                    .catalogSyncClientImpl(TestCatalogSyncImpl.class.getName())
                    .catalogProperties(Collections.emptyMap())
                    .build())
            .catalogTableIdentifier(
                new ThreePartHierarchicalTableIdentifier("target-database", "target-tableName"))
            .build();
    CatalogSyncClient catalogSyncClient =
        CatalogConversionFactory.getInstance()
            .createCatalogSyncClient(
                targetCatalogConfig.getCatalogConfig(), "TABLE_FORMAT", new Configuration());
    assertEquals(catalogSyncClient.getClass().getName(), TestCatalogSyncImpl.class.getName());
  }

  @Test
  void createCatalogSyncClientForCatalogType() {
    TargetCatalogConfig targetCatalogConfig =
        TargetCatalogConfig.builder()
            .catalogConfig(
                ExternalCatalogConfig.builder()
                    .catalogId("catalogId")
                    .catalogType(ITTestUtils.TEST_CATALOG_TYPE)
                    .catalogProperties(Collections.emptyMap())
                    .build())
            .catalogTableIdentifier(
                new ThreePartHierarchicalTableIdentifier("target-database", "target-tableName"))
            .build();
    CatalogSyncClient catalogSyncClient =
        CatalogConversionFactory.getInstance()
            .createCatalogSyncClient(
                targetCatalogConfig.getCatalogConfig(), "TABLE_FORMAT", new Configuration());
    assertEquals(catalogSyncClient.getClass().getName(), TestCatalogSyncImpl.class.getName());
  }
}
