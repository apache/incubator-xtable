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
 
package org.apache.xtable;

import static org.apache.xtable.GenericTable.getTableName;
import static org.apache.xtable.model.storage.TableFormat.DELTA;
import static org.apache.xtable.model.storage.TableFormat.HUDI;
import static org.apache.xtable.model.storage.TableFormat.ICEBERG;
import static org.apache.xtable.model.storage.TableFormat.PAIMON;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.sql.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.xtable.conversion.ConversionConfig;
import org.apache.xtable.conversion.ConversionSourceProvider;
import org.apache.xtable.model.sync.SyncMode;

/** End-to-end conversion across every source format, sync mode and partitioning combination. */
public class ITConversionController extends ConversionControllerTestBase {

  private static Stream<Arguments> generateTestParametersForFormatsSyncModesAndPartitioning() {
    List<Arguments> arguments = new ArrayList<>();
    for (String sourceFormat : Arrays.asList(HUDI, DELTA, ICEBERG, PAIMON)) {
      for (SyncMode syncMode : SyncMode.values()) {
        for (boolean isPartitioned : new boolean[] {true, false}) {
          arguments.add(Arguments.of(sourceFormat, syncMode, isPartitioned));
        }
      }
    }
    return arguments.stream();
  }

  /*
   * This test has the following steps at a high level.
   * 1. Insert few records.
   * 2. Upsert few records.
   * 3. Delete few records.
   * 4. Insert records with new columns.
   * 5. Insert records in a new partition if table is partitioned.
   * 6. drop a partition if table is partitioned.
   * 7. Insert records in the dropped partition again if table is partitioned.
   */
  @ParameterizedTest
  @MethodSource("generateTestParametersForFormatsSyncModesAndPartitioning")
  public void testVariousOperations(
      String sourceTableFormat, SyncMode syncMode, boolean isPartitioned) {
    String tableName = getTableName();
    List<String> targetTableFormats = getOtherFormats(sourceTableFormat);
    if (sourceTableFormat.equals(PAIMON)) {
      // TODO: Hudi 1.x target is not supported for un-partitioned Paimon source.
      targetTableFormats =
          targetTableFormats.stream().filter(fmt -> !fmt.equals(HUDI)).collect(Collectors.toList());
    }
    String partitionConfig = null;
    if (isPartitioned) {
      partitionConfig = "level:VALUE";
    }
    ConversionSourceProvider<?> conversionSourceProvider =
        getConversionSourceProvider(sourceTableFormat);
    List<?> insertRecords;
    try (GenericTable table =
        GenericTable.getInstance(
            tableName, tempDir, sparkSession, jsc, sourceTableFormat, isPartitioned)) {
      insertRecords = table.insertRows(100);

      ConversionConfig conversionConfig =
          getTableSyncConfig(
              sourceTableFormat,
              syncMode,
              tableName,
              table,
              targetTableFormats,
              partitionConfig,
              null);
      conversionController.sync(conversionConfig, conversionSourceProvider);
      checkDatasetEquivalence(sourceTableFormat, table, targetTableFormats, 100);

      // make multiple commits and then sync
      table.insertRows(100);
      table.upsertRows(insertRecords.subList(0, 20));
      conversionController.sync(conversionConfig, conversionSourceProvider);
      checkDatasetEquivalence(sourceTableFormat, table, targetTableFormats, 200);

      table.deleteRows(insertRecords.subList(30, 50));
      conversionController.sync(conversionConfig, conversionSourceProvider);
      checkDatasetEquivalence(sourceTableFormat, table, targetTableFormats, 180);
      checkDatasetEquivalenceWithFilter(
          sourceTableFormat,
          table,
          targetTableFormats,
          table.getFilterQuery(),
          Collections.emptyMap());
    }

    try (GenericTable tableWithUpdatedSchema =
        GenericTable.getInstanceWithAdditionalColumns(
            tableName, tempDir, sparkSession, jsc, sourceTableFormat, isPartitioned)) {
      ConversionConfig conversionConfig =
          getTableSyncConfig(
              sourceTableFormat,
              syncMode,
              tableName,
              tableWithUpdatedSchema,
              targetTableFormats,
              partitionConfig,
              null);
      List<Row> insertsAfterSchemaUpdate = tableWithUpdatedSchema.insertRows(100);
      tableWithUpdatedSchema.reload();
      conversionController.sync(conversionConfig, conversionSourceProvider);
      checkDatasetEquivalence(sourceTableFormat, tableWithUpdatedSchema, targetTableFormats, 280);

      tableWithUpdatedSchema.deleteRows(insertsAfterSchemaUpdate.subList(60, 90));
      conversionController.sync(conversionConfig, conversionSourceProvider);
      checkDatasetEquivalence(sourceTableFormat, tableWithUpdatedSchema, targetTableFormats, 250);

      if (isPartitioned) {
        // Adds new partition.
        tableWithUpdatedSchema.insertRecordsForSpecialPartition(50);
        conversionController.sync(conversionConfig, conversionSourceProvider);
        checkDatasetEquivalence(sourceTableFormat, tableWithUpdatedSchema, targetTableFormats, 300);

        // Drops partition.
        tableWithUpdatedSchema.deleteSpecialPartition();
        conversionController.sync(conversionConfig, conversionSourceProvider);
        checkDatasetEquivalence(sourceTableFormat, tableWithUpdatedSchema, targetTableFormats, 250);

        // Insert records to the dropped partition again.
        tableWithUpdatedSchema.insertRecordsForSpecialPartition(50);
        conversionController.sync(conversionConfig, conversionSourceProvider);
        checkDatasetEquivalence(sourceTableFormat, tableWithUpdatedSchema, targetTableFormats, 300);
      }
    }
  }
}
