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

import static org.apache.xtable.testutil.ITTestUtils.TEST_CATALOG_TYPE;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Collections;
import java.util.UUID;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.xtable.conversion.ExternalCatalogConfig;
import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.testutil.ITTestUtils;

class TestExternalCatalogConfigFactory {

  @Test
  void testFromCatalogType() {
    ExternalCatalogConfig externalCatalogConfig =
        ExternalCatalogConfigFactory.fromCatalogType(
            TEST_CATALOG_TYPE, UUID.randomUUID().toString(), Collections.emptyMap());
    Assertions.assertEquals(
        ITTestUtils.TestCatalogSyncImpl.class.getName(),
        externalCatalogConfig.getCatalogSyncClientImpl());
    Assertions.assertEquals(
        ITTestUtils.TestCatalogConversionSourceImpl.class.getName(),
        externalCatalogConfig.getCatalogConversionSourceImpl());
  }

  @Test
  void testFromCatalogTypeNotFound() {
    Assertions.assertThrows(
        NotSupportedException.class,
        () ->
            ExternalCatalogConfigFactory.fromCatalogType(
                "invalid", UUID.randomUUID().toString(), Collections.emptyMap()));
  }
}
