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

import java.util.Map;

import org.apache.xtable.conversion.ExternalCatalogConfig;

/** A factory class which returns {@link ExternalCatalogConfig} based on catalogType. */
public class ExternalCatalogConfigFactory {

  public static ExternalCatalogConfig fromCatalogType(
      String catalogType, String catalogId, Map<String, String> properties) {
    // TODO: Choose existing implementation based on catalogType.
    String catalogSyncClientImpl = "";
    String catalogConversionSourceImpl = "";
    return ExternalCatalogConfig.builder()
        .catalogType(catalogType)
        .catalogSyncClientImpl(catalogSyncClientImpl)
        .catalogConversionSourceImpl(catalogConversionSourceImpl)
        .catalogId(catalogId)
        .catalogProperties(properties)
        .build();
  }
}
