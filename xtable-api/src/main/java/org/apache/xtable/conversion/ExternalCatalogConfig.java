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
 
package org.apache.xtable.conversion;

import java.util.Collections;
import java.util.Map;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

/**
 * Defines the configuration for an external catalog, user needs to populate at-least one of {@link
 * ExternalCatalogConfig#catalogType} or {@link ExternalCatalogConfig#catalogSyncClientImpl}
 */
@Value
@Builder
public class ExternalCatalogConfig {
  /**
   * A user-defined unique identifier for the catalog, allows user to sync table to multiple
   * catalogs of the same name/type eg: HMS catalog with url1, HMS catalog with url2
   */
  @NonNull String catalogId;

  /**
   * The type of the catalog. If the catalogType implementation exists in XTable, the implementation
   * class will be inferred.
   */
  String catalogType;

  /**
   * (Optional) A fully qualified class name that implements the interface for {@link
   * org.apache.xtable.spi.sync.CatalogSyncClient}, it can be used if the implementation for
   * catalogType doesn't exist in XTable.
   */
  String catalogSyncClientImpl;

  /**
   * (Optional) A fully qualified class name that implements the interface for {@link
   * org.apache.xtable.spi.extractor.CatalogConversionSource} it can be used if the implementation
   * for catalogType doesn't exist in XTable.
   */
  String catalogConversionSourceImpl;

  /**
   * (Optional) A fully qualified class name that implements the interface for {@link
   * org.apache.xtable.spi.sync.CatalogAccessControlPolicySyncClient} it can be used if the
   * implementation for catalogType doesn't exist in XTable.
   */
  String catalogPolicySyncClientImpl;

  /**
   * The properties for this catalog, used for providing any custom behaviour during catalog sync
   */
  @NonNull @Builder.Default Map<String, String> catalogProperties = Collections.emptyMap();
}
