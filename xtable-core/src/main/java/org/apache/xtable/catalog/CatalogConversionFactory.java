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

import java.util.ServiceLoader;
import java.util.function.Function;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import org.apache.xtable.conversion.ExternalCatalogConfig;
import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.reflection.ReflectionUtils;
import org.apache.xtable.spi.extractor.CatalogConversionSource;
import org.apache.xtable.spi.sync.CatalogAccessControlPolicySyncClient;
import org.apache.xtable.spi.sync.CatalogSyncClient;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class CatalogConversionFactory {
  private static final CatalogConversionFactory INSTANCE = new CatalogConversionFactory();

  public static CatalogConversionFactory getInstance() {
    return INSTANCE;
  }

  /**
   * Returns an implementation class for {@link CatalogConversionSource} that's used for converting
   * table definitions in the catalog to {@link org.apache.xtable.conversion.SourceTable} object.
   *
   * @param sourceCatalogConfig configuration for the source catalog
   * @param configuration hadoop configuration
   */
  public static CatalogConversionSource createCatalogConversionSource(
      ExternalCatalogConfig sourceCatalogConfig, Configuration configuration) {
    if (!StringUtils.isEmpty(sourceCatalogConfig.getCatalogType())) {
      CatalogConversionSource catalogConversionSource =
          findInstanceByCatalogType(
              CatalogConversionSource.class,
              sourceCatalogConfig.getCatalogType(),
              CatalogConversionSource::getCatalogType);
      catalogConversionSource.init(sourceCatalogConfig, configuration);
      return catalogConversionSource;
    }
    return ReflectionUtils.createInstanceOfClass(
        sourceCatalogConfig.getCatalogConversionSourceImpl(), sourceCatalogConfig, configuration);
  }

  /**
   * Returns an implementation class for {@link CatalogSyncClient} that's used for syncing {@link
   * org.apache.xtable.conversion.TargetTable} to a catalog.
   *
   * @param targetCatalogConfig configuration for the target catalog
   * @param configuration hadoop configuration
   */
  public <TABLE> CatalogSyncClient<TABLE> createCatalogSyncClient(
      ExternalCatalogConfig targetCatalogConfig, String tableFormat, Configuration configuration) {
    if (!StringUtils.isEmpty(targetCatalogConfig.getCatalogType())) {
      CatalogSyncClient catalogSyncClient =
          findInstanceByCatalogType(
              CatalogSyncClient.class,
              targetCatalogConfig.getCatalogType(),
              CatalogSyncClient::getCatalogType);
      catalogSyncClient.init(targetCatalogConfig, tableFormat, configuration);
      return catalogSyncClient;
    }
    return ReflectionUtils.createInstanceOfClass(
        targetCatalogConfig.getCatalogSyncClientImpl(),
        targetCatalogConfig,
        tableFormat,
        configuration);
  }

  /**
   * Returns an implementation class for {@link CatalogAccessControlPolicySyncClient} that'll be
   * used for fetching / syncing policies from / to catalog
   *
   * @param catalogConfig configuration for the catalog
   * @param configuration hadoop configuration
   */
  public CatalogAccessControlPolicySyncClient createCatalogPolicySyncClient(
      ExternalCatalogConfig catalogConfig, Configuration configuration) {
    return ReflectionUtils.createInstanceOfClass(
        catalogConfig.getCatalogPolicySyncClientImpl(), catalogConfig, configuration);
  }

  private static <T> T findInstanceByCatalogType(
      Class<T> serviceClass, String catalogType, Function<T, String> catalogTypeExtractor) {
    ServiceLoader<T> loader = ServiceLoader.load(serviceClass);
    for (T instance : loader) {
      String instanceCatalogType = catalogTypeExtractor.apply(instance);
      if (catalogType.equals(instanceCatalogType)) {
        return instance;
      }
    }
    throw new NotSupportedException("catalogType is not yet supported: " + catalogType);
  }
}
