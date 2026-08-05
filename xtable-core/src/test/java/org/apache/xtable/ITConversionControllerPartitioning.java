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

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.xtable.conversion.ConversionConfig;
import org.apache.xtable.conversion.ConversionController;
import org.apache.xtable.conversion.ConversionSourceProvider;
import org.apache.xtable.iceberg.TestIcebergDataHelper;
import org.apache.xtable.model.sync.SyncMode;

/** Conversion of partitioned tables, including non-trivial partition transforms. */
public class ITConversionControllerPartitioning extends ConversionControllerTestBase {

  private static Stream<Arguments> provideArgsForPartitionTesting() {
    String timestampFilter =
        String.format(
            "timestamp_micros_nullable_field < timestamp_millis(%s)",
            Instant.now().truncatedTo(ChronoUnit.DAYS).minus(2, ChronoUnit.DAYS).toEpochMilli());
    String levelFilter = "level = 'INFO'";
    String severityFilter = "severity = 1";
    String timestampAndLevelFilter = String.format("%s and %s", timestampFilter, levelFilter);
    return Stream.of(
        Arguments.of(
            buildArgsForPartition(
                HUDI, Arrays.asList(ICEBERG, DELTA), "level:SIMPLE", "level:VALUE", levelFilter)),
        Arguments.of(
            buildArgsForPartition(
                DELTA, Arrays.asList(ICEBERG, HUDI), null, "level:VALUE", levelFilter)),
        Arguments.of(
            buildArgsForPartition(
                ICEBERG, Arrays.asList(DELTA, HUDI), null, "level:VALUE", levelFilter)),
        // TODO(hudi-1.2): re-enable the nested partition column case (HUDI -> ICEBERG partitioned
        // on
        // "nested_record.level"). Hudi 1.2's HoodieFileGroupReaderBasedFileFormat is the only batch
        // reader and it converts the partition column into a top-level Avro field named
        // "nested_record.level", which Avro rejects ("Illegal character in: nested_record.level").
        // Delta is excluded here anyway since it does not support nested partition columns.
        // Arguments.of(
        //     buildArgsForPartition(
        //         HUDI,
        //         Arrays.asList(ICEBERG),
        //         "nested_record.level:SIMPLE",
        //         "nested_record.level:VALUE",
        //         nestedLevelFilter)),
        Arguments.of(
            buildArgsForPartition(
                HUDI,
                Arrays.asList(ICEBERG, DELTA),
                "severity:SIMPLE",
                "severity:VALUE",
                severityFilter)),
        Arguments.of(
            buildArgsForPartition(
                HUDI,
                Arrays.asList(ICEBERG, DELTA),
                "timestamp_micros_nullable_field:TIMESTAMP,level:SIMPLE",
                "timestamp_micros_nullable_field:DAY:yyyy/MM/dd,level:VALUE",
                timestampAndLevelFilter,
                getAdditionalHudiReadOptions())));
  }

  @ParameterizedTest
  @MethodSource("provideArgsForPartitionTesting")
  public void testPartitionedData(TableFormatPartitionDataHolder tableFormatPartitionDataHolder) {
    String tableName = getTableName();
    String sourceTableFormat = tableFormatPartitionDataHolder.getSourceTableFormat();
    List<String> targetTableFormats = tableFormatPartitionDataHolder.getTargetTableFormats();
    Optional<String> hudiPartitionConfig = tableFormatPartitionDataHolder.getHudiSourceConfig();
    String xTablePartitionConfig = tableFormatPartitionDataHolder.getXTablePartitionConfig();
    String filter = tableFormatPartitionDataHolder.getFilter();
    ConversionSourceProvider<?> conversionSourceProvider =
        getConversionSourceProvider(sourceTableFormat);
    GenericTable table;
    if (hudiPartitionConfig.isPresent()) {
      table =
          GenericTable.getInstanceWithCustomPartitionConfig(
              tableName, tempDir, jsc, sourceTableFormat, hudiPartitionConfig.get());
    } else {
      table =
          GenericTable.getInstance(tableName, tempDir, sparkSession, jsc, sourceTableFormat, true);
    }
    try (GenericTable tableToClose = table) {
      ConversionConfig conversionConfig =
          getTableSyncConfig(
              sourceTableFormat,
              SyncMode.INCREMENTAL,
              tableName,
              table,
              targetTableFormats,
              xTablePartitionConfig,
              null);
      tableToClose.insertRows(100);
      conversionController.sync(conversionConfig, conversionSourceProvider);
      // Do a second sync to force the test to read back the metadata it wrote earlier
      tableToClose.insertRows(100);
      conversionController.sync(conversionConfig, conversionSourceProvider);

      checkDatasetEquivalenceWithFilter(
          sourceTableFormat,
          tableToClose,
          targetTableFormats,
          filter,
          tableFormatPartitionDataHolder.getAdditionalHudiReadOptions());
    }
  }

  @Test
  void otherIcebergPartitionTypes() {
    String tableName = getTableName();
    ConversionController conversionController = new ConversionController(jsc.hadoopConfiguration());
    List<String> targetTableFormats = Collections.singletonList(DELTA);

    ConversionSourceProvider<?> conversionSourceProvider = getConversionSourceProvider(ICEBERG);
    try (TestIcebergTable table =
        new TestIcebergTable(
            tableName,
            tempDir,
            jsc.hadoopConfiguration(),
            "id",
            Arrays.asList("level", "string_field"),
            TestIcebergDataHelper.SchemaType.COMMON)) {
      table.insertRows(100);

      ConversionConfig conversionConfig =
          getTableSyncConfig(
              ICEBERG, SyncMode.FULL, tableName, table, targetTableFormats, null, null);
      conversionController.sync(conversionConfig, conversionSourceProvider);
      checkDatasetEquivalence(ICEBERG, table, targetTableFormats, 100);
      // Query with filter to assert partition does not impact ability to query
      checkDatasetEquivalenceWithFilter(
          ICEBERG,
          table,
          targetTableFormats,
          "level == 'INFO' AND string_field > 'abc'",
          Collections.emptyMap());
    }
  }
}
