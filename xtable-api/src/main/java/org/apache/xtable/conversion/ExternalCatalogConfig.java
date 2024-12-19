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
 * Defines the configuration for an external catalog, user needs to populate at-least one of
 * catalogType or catalogImpl
 */
@Value
@Builder
public class ExternalCatalogConfig {
  /** The name of the catalog, it also acts as a unique identifier for each catalog */
  @NonNull String catalogId;

  /**
   * The type of the catalog. If the catalogType implementation exists in XTable, the implementation
   * class will be inferred.
   */
  String catalogType;

  /**
   * (Optional) A fully qualified class name that implements the interfaces for CatalogSyncClient,
   * it can be used if the implementation for catalogType doesn't exist in XTable.
   */
  String catalogImpl;

  /**
   * The properties for each catalog, used for providing any custom behaviour during catalog sync
   */
  @NonNull @Builder.Default Map<String, String> catalogOptions = Collections.emptyMap();
}
