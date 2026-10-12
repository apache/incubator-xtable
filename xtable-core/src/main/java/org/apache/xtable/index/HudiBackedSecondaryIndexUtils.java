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
 
package org.apache.xtable.index;

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_SECONDARY_INDEX_PREFIX;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.hadoop.conf.Configuration;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;

import com.google.common.base.Preconditions;

import org.apache.xtable.conversion.ConversionSourceProvider;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.spi.extractor.ConversionSource;

/**
 * Shared parts of {@link HudiBackedSecondaryIndexWriter} and {@link
 * HudiBackedSecondaryIndexReader}.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class HudiBackedSecondaryIndexUtils {
  // types whose values Hudi renders as their plain string, a day count or microseconds
  private static final Set<InternalType> SUPPORTED_KEY_TYPES =
      EnumSet.of(
          InternalType.STRING,
          InternalType.INT,
          InternalType.LONG,
          InternalType.BOOLEAN,
          InternalType.FLOAT,
          InternalType.DOUBLE,
          InternalType.DECIMAL,
          InternalType.DATE,
          InternalType.TIMESTAMP,
          InternalType.TIMESTAMP_NTZ);

  /** Reads the current schema of the source table through its conversion source. */
  static InternalSchema readSchema(
      ConversionSourceProvider<?> sourceProvider, SourceTable sourceTable) {
    try (ConversionSource<?> conversionSource =
        sourceProvider.getConversionSourceInstance(sourceTable)) {
      return conversionSource.getCurrentTable().getReadSchema();
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed to close the conversion source of " + sourceTable.getBasePath(), e);
    }
  }

  /**
   * Returns the field of the given column, and checks that the index supports its type. Hudi
   * renders a BINARY, FIXED or UUID value as an object reference, so those types are not supported.
   */
  static InternalField getIndexableField(InternalSchema schema, String column) {
    InternalField field =
        schema.getAllFields().stream()
            .filter(candidate -> candidate.getPath().equals(column))
            .findFirst()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        String.format("Column %s is not in the table schema", column)));
    InternalType type = field.getSchema().getDataType();
    Preconditions.checkArgument(
        SUPPORTED_KEY_TYPES.contains(type),
        String.format("A secondary index on column %s of type %s is not supported", column, type));
    return field;
  }

  /** Checks that a table passed to an index is the table the index was created for. */
  static void checkIndexedTable(String tableLocation, SourceTable table) {
    Preconditions.checkArgument(
        tableLocation.equals(table.getBasePath()),
        String.format(
            "The index belongs to table %s but got table %s", tableLocation, table.getBasePath()));
  }

  static HoodieTableMetaClient getMetaClient(String dataPath, Configuration configuration) {
    HoodieStorage storage = new HoodieHadoopStorage(dataPath, configuration);
    return HoodieTableMetaClient.builder().setStorage(storage).setBasePath(dataPath).build();
  }

  /** Returns the columns whose index exists, or an empty set when there is no Hudi table yet. */
  static Set<String> getIndexedColumns(String dataPath, Configuration configuration) {
    try {
      return getIndexedColumns(getMetaClient(dataPath, configuration));
    } catch (TableNotFoundException e) {
      return Collections.emptySet();
    }
  }

  /**
   * Returns the columns whose index build has completed. A metadata table partition directory can
   * exist while the build is in flight or after it failed, so the directory alone does not show
   * that the index is usable.
   */
  static Set<String> getIndexedColumns(HoodieTableMetaClient metaClient) {
    // The commit that creates the metadata table marks its partitions complete before the data
    // commit completes, so the partitions of a table without a completed commit are not an index.
    if (metaClient.getActiveTimeline().getWriteTimeline().filterCompletedInstants().empty()) {
      return Collections.emptySet();
    }
    return metaClient.getTableConfig().getMetadataPartitions().stream()
        .filter(partition -> partition.startsWith(PARTITION_NAME_SECONDARY_INDEX_PREFIX))
        .map(partition -> partition.substring(PARTITION_NAME_SECONDARY_INDEX_PREFIX.length()))
        .collect(Collectors.toSet());
  }
}
