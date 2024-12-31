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
import java.util.ServiceLoader;
import java.util.function.Function;

import org.apache.xtable.conversion.ExternalCatalogConfig;
import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.spi.extractor.CatalogConversionSource;
import org.apache.xtable.spi.sync.CatalogSyncClient;

/** A factory class which returns {@link ExternalCatalogConfig} based on catalogType. */
public class ExternalCatalogConfigFactory {

  public static ExternalCatalogConfig fromCatalogType(
      String catalogType, String catalogId, Map<String, String> properties) {
    String catalogSyncClientImpl =
        findImplClassName(CatalogSyncClient.class, catalogType, CatalogSyncClient::getCatalogType);
    String catalogConversionSourceImpl =
        findImplClassName(
            CatalogConversionSource.class, catalogType, CatalogConversionSource::getCatalogType);
    ;
    return ExternalCatalogConfig.builder()
        .catalogType(catalogType)
        .catalogSyncClientImpl(catalogSyncClientImpl)
        .catalogConversionSourceImpl(catalogConversionSourceImpl)
        .catalogId(catalogId)
        .catalogProperties(properties)
        .build();
  }

  private static <T> String findImplClassName(
      Class<T> serviceClass, String catalogType, Function<T, String> catalogTypeExtractor) {
    ServiceLoader<T> loader = ServiceLoader.load(serviceClass);
    for (T instance : loader) {
      String instanceCatalogType = catalogTypeExtractor.apply(instance);
      if (catalogType.equals(instanceCatalogType)) {
        return instance.getClass().getName();
      }
    }
    throw new NotSupportedException("catalogType is not yet supported: " + catalogType);
  }
}
