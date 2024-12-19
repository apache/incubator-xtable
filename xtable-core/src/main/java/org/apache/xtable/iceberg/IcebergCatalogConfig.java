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
 
package org.apache.xtable.iceberg;

import java.util.Collections;
import java.util.Map;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

import org.apache.xtable.conversion.CatalogConfig;

/**
 * Iceberg requires a catalog to perform any operation, if no catalog is provided the default
 * catalog (HadoopCatalog or storage based catalog) is used. For syncing iceberg to multiple
 * catalogs, you can use {@link org.apache.xtable.conversion.ExternalCatalogConfig} instead which
 * allows syncing the latest version of iceberg metadata to multiple catalogs.
 */
@Value
@Builder
public class IcebergCatalogConfig implements CatalogConfig {
  @NonNull String catalogName;
  @NonNull String catalogImpl;
  @NonNull @Builder.Default Map<String, String> catalogOptions = Collections.emptyMap();
}
