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

import java.util.Optional;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.hadoop.conf.Configuration;

import org.apache.hudi.sync.common.model.PartitionValueExtractor;

import org.apache.xtable.hudi.catalog.HudiCatalogPartitionSyncTool;
import org.apache.xtable.model.catalog.CatalogTableIdentifier;
import org.apache.xtable.model.catalog.HierarchicalTableIdentifier;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.reflection.ReflectionUtils;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class CatalogUtils {

  public static HierarchicalTableIdentifier toHierarchicalTableIdentifier(
      CatalogTableIdentifier tableIdentifier) {
    if (tableIdentifier instanceof HierarchicalTableIdentifier) {
      return (HierarchicalTableIdentifier) tableIdentifier;
    }
    throw new IllegalArgumentException(
        "Invalid tableIdentifier implementation: " + tableIdentifier.getClass().getName());
  }

  public static Optional<CatalogPartitionSyncTool> getPartitionSyncTool(
      String tableFormat,
      String partitionValueExtractorClass,
      CatalogPartitionSyncOperations catalogPartitionSyncOperations,
      Configuration configuration) {

    if (partitionValueExtractorClass.isEmpty()) {
      return Optional.empty();
    }

    // In Iceberg and Delta, partitions are automatically synchronized with catalogs when
    // table metadata is updated. However, for Hudi, we need to sync them manually
    if (tableFormat.equals(TableFormat.HUDI)) {
      PartitionValueExtractor partitionValueExtractor =
          ReflectionUtils.createInstanceOfClass(partitionValueExtractorClass);
      return Optional.of(
          new HudiCatalogPartitionSyncTool(
              catalogPartitionSyncOperations, partitionValueExtractor, configuration));
    }
    return Optional.empty();
  }
}
