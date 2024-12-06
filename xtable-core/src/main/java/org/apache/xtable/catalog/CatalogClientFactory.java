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

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.xtable.conversion.ExternalCatalog;
import org.apache.xtable.model.catalog.CatalogType;
import org.apache.xtable.spi.sync.CatalogSyncClient;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class CatalogClientFactory {
  private static final CatalogClientFactory INSTANCE = new CatalogClientFactory();

  public static CatalogClientFactory getInstance() {
    return INSTANCE;
  }

  public CatalogSyncClient createForCatalogAndFormat(
      String tableFormat, ExternalCatalog externalCatalog) {
    if (externalCatalog.getCatalogType() == CatalogType.HMS) {
      // TODO: Create CatalogSyncOperations based on tableFormat and catalogType.
    }
    return null;
  }
}
