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

import static org.apache.xtable.hudi.HudiSourceConfig.PARTITION_FIELD_SPEC_CONFIG;
import static org.apache.xtable.hudi.HudiTestUtil.PartitionConfig;
import static org.apache.xtable.model.storage.TableFormat.DELTA;
import static org.apache.xtable.model.storage.TableFormat.HUDI;
import static org.apache.xtable.model.storage.TableFormat.ICEBERG;
import static org.apache.xtable.model.storage.TableFormat.PAIMON;
import static org.apache.xtable.model.storage.TableFormat.PARQUET;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.Builder;
import lombok.Value;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.provider.Arguments;

import org.apache.hudi.client.HoodieReadClient;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.timeline.HoodieInstant;

import org.apache.iceberg.Snapshot;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.xtable.conversion.ConversionConfig;
import org.apache.xtable.conversion.ConversionController;
import org.apache.xtable.conversion.ConversionSourceProvider;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.conversion.TargetTable;
import org.apache.xtable.delta.DeltaConversionSourceProvider;
import org.apache.xtable.hudi.HudiConversionSourceProvider;
import org.apache.xtable.hudi.HudiTestUtil;
import org.apache.xtable.iceberg.IcebergConversionSourceProvider;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.model.sync.SyncMode;
import org.apache.xtable.paimon.PaimonConversionSourceProvider;

/**
 * Shared Spark fixture and assertion helpers for the {@code ITConversionController*} integration
 * tests.
 *
 * <p>These tests were originally a single class. Failsafe is configured with {@code
 * reuseForks=false}, so one test class occupies exactly one fork and its tests run serially no
 * matter how high {@code forkCount} is; the single class was therefore the critical path of the
 * whole build. Splitting them across several classes lets the existing forks run them concurrently.
 * Each subclass gets its own JVM and therefore its own {@link SparkSession}, which is why the
 * fixture lives here rather than being shared across classes.
 */
abstract class ConversionControllerTestBase {
  @TempDir public static Path tempDir;

  private static final DateTimeFormatter DATE_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.of("UTC"));
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  protected static JavaSparkContext jsc;
  protected static SparkSession sparkSession;
  protected static ConversionController conversionController;

  @BeforeAll
  public static void setupOnce() {
    SparkConf sparkConf = HudiTestUtil.getSparkConf(tempDir);

    sparkSession =
        SparkSession.builder().config(HoodieReadClient.addHoodieSupport(sparkConf)).getOrCreate();
    sparkSession
        .sparkContext()
        .hadoopConfiguration()
        .set("parquet.avro.write-old-list-structure", "false");

    jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
    conversionController = new ConversionController(jsc.hadoopConfiguration());
  }

  @AfterAll
  public static void teardown() {
    if (jsc != null) {
      jsc.close();
    }
    if (sparkSession != null) {
      sparkSession.close();
    }
  }

  protected static Stream<Arguments> testCasesWithSyncModes() {
    return Stream.of(Arguments.of(SyncMode.INCREMENTAL), Arguments.of(SyncMode.FULL));
  }

  protected static Stream<Arguments> testCasesWithPartitioningAndSyncModes() {
    return addBasicPartitionCases(testCasesWithSyncModes());
  }

  protected ConversionSourceProvider<?> getConversionSourceProvider(String sourceTableFormat) {
    switch (sourceTableFormat.toUpperCase()) {
      case HUDI:
        {
          ConversionSourceProvider<HoodieInstant> hudiConversionSourceProvider =
              new HudiConversionSourceProvider();
          hudiConversionSourceProvider.init(jsc.hadoopConfiguration());
          return hudiConversionSourceProvider;
        }
      case DELTA:
        {
          ConversionSourceProvider<Long> deltaConversionSourceProvider =
              new DeltaConversionSourceProvider();
          deltaConversionSourceProvider.init(jsc.hadoopConfiguration());
          return deltaConversionSourceProvider;
        }
      case ICEBERG:
        {
          ConversionSourceProvider<Snapshot> icebergConversionSourceProvider =
              new IcebergConversionSourceProvider();
          icebergConversionSourceProvider.init(jsc.hadoopConfiguration());
          return icebergConversionSourceProvider;
        }
      case PAIMON:
        {
          ConversionSourceProvider<org.apache.paimon.Snapshot> paimonConversionSourceProvider =
              new PaimonConversionSourceProvider();
          paimonConversionSourceProvider.init(jsc.hadoopConfiguration());
          return paimonConversionSourceProvider;
        }
      default:
        throw new IllegalArgumentException("Unsupported source format: " + sourceTableFormat);
    }
  }

  protected static List<String> getOtherFormats(String sourceTableFormat) {
    return Arrays.stream(TableFormat.values())
        .filter(fmt -> !fmt.equals(sourceTableFormat))
        .filter(fmt -> !fmt.equals(PAIMON)) // Paimon target is not supported yet
        .filter(fmt -> !fmt.equals(PARQUET)) // upserts/inserts are not supported in Parquet
        .collect(Collectors.toList());
  }

  protected Map<String, String> getTimeTravelOption(String tableFormat, Instant time) {
    Map<String, String> options = new HashMap<>();
    switch (tableFormat) {
      case HUDI:
        options.put("as.of.instant", DATE_FORMAT.format(time));
        break;
      case ICEBERG:
        options.put("as-of-timestamp", String.valueOf(time.toEpochMilli()));
        break;
      case DELTA:
        options.put("timestampAsOf", DATE_FORMAT.format(time));
        break;
      default:
        throw new IllegalArgumentException("Unknown table format: " + tableFormat);
    }
    return options;
  }

  protected void checkDatasetEquivalenceWithFilter(
      String sourceFormat,
      GenericTable<?, ?> sourceTable,
      List<String> targetFormats,
      String filter,
      Map<String, String> additionalHudiReadOptions) {
    Map<String, Map<String, String>> targetOptions =
        targetFormats.contains(HUDI)
            ? Collections.singletonMap(HUDI, additionalHudiReadOptions)
            : Collections.emptyMap();
    checkDatasetEquivalence(
        sourceFormat,
        sourceTable,
        HUDI.equals(sourceFormat) ? additionalHudiReadOptions : Collections.emptyMap(),
        targetFormats,
        targetOptions,
        null,
        filter);
  }

  protected void checkDatasetEquivalence(
      String sourceFormat,
      GenericTable<?, ?> sourceTable,
      List<String> targetFormats,
      Integer expectedCount) {
    checkDatasetEquivalence(
        sourceFormat,
        sourceTable,
        Collections.emptyMap(),
        targetFormats,
        Collections.emptyMap(),
        expectedCount,
        "1 = 1");
  }

  protected void checkDatasetEquivalence(
      String sourceFormat,
      GenericTable<?, ?> sourceTable,
      Map<String, String> sourceOptions,
      List<String> targetFormats,
      Map<String, Map<String, String>> targetOptions,
      Integer expectedCount) {
    checkDatasetEquivalence(
        sourceFormat,
        sourceTable,
        sourceOptions,
        targetFormats,
        targetOptions,
        expectedCount,
        "1 = 1");
  }

  protected void checkDatasetEquivalence(
      String sourceFormat,
      GenericTable<?, ?> sourceTable,
      Map<String, String> sourceOptions,
      List<String> targetFormats,
      Map<String, Map<String, String>> targetOptions,
      Integer expectedCount,
      String filterCondition) {
    Dataset<Row> sourceRows =
        sparkSession
            .read()
            .options(sourceOptions)
            .format(sourceFormat.toLowerCase())
            .load(sourceTable.getBasePath())
            .orderBy(sourceTable.getOrderByColumn())
            .filter(filterCondition);
    Map<String, Dataset<Row>> targetRowsByFormat =
        targetFormats.stream()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    targetFormat -> {
                      Map<String, String> finalTargetOptions =
                          targetOptions.getOrDefault(targetFormat, Collections.emptyMap());
                      if (targetFormat.equals(HUDI)) {
                        finalTargetOptions = new HashMap<>(finalTargetOptions);
                        finalTargetOptions.put(HoodieMetadataConfig.ENABLE.key(), "true");
                        finalTargetOptions.put(
                            "hoodie.datasource.read.extract.partition.values.from.path", "true");
                      }
                      return sparkSession
                          .read()
                          .options(finalTargetOptions)
                          .format(targetFormat.toLowerCase())
                          .load(sourceTable.getDataPath())
                          .orderBy(sourceTable.getOrderByColumn())
                          .filter(filterCondition);
                    }));

    List<String> sourceRowsList =
        sourceRows
            .selectExpr(getSelectColumnsArr(sourceTable.getColumnsToSelect(), sourceFormat))
            .toJSON()
            .collectAsList();
    targetRowsByFormat.forEach(
        (targetFormat, targetRows) -> {
          List<String> targetRowsList =
              targetRows
                  .selectExpr(getSelectColumnsArr(sourceTable.getColumnsToSelect(), targetFormat))
                  .toJSON()
                  .collectAsList();
          assertEquals(
              sourceRowsList.size(),
              targetRowsList.size(),
              String.format(
                  "Datasets have different row counts when reading from Spark. Source: %s, Target: %s",
                  sourceFormat, targetFormat));
          // sanity check the count to ensure test is set up properly
          if (expectedCount != null) {
            assertEquals(expectedCount, sourceRowsList.size());
          } else {
            // if count is not known ahead of time, ensure datasets are non-empty
            assertFalse(sourceRowsList.isEmpty());
          }

          if (containsUUIDFields(sourceRowsList) && containsUUIDFields(targetRowsList)) {
            compareDatasetWithUUID(sourceRowsList, targetRowsList);
          } else {
            assertEquals(
                sourceRowsList,
                targetRowsList,
                String.format(
                    "Datasets are not equivalent when reading from Spark. Source: %s, Target: %s",
                    sourceFormat, targetFormat));
          }
        });
  }

  /**
   * Extra Hudi read options for partition tests that need them. Hudi 1.2 defaults to lazy
   * file-index listing, which fails to parse partition values for some partition transforms (e.g.
   * timestamp-based partitions); forcing eager listing avoids that. Passed only for the cases that
   * require it via {@link #buildArgsForPartition}.
   */
  protected static Map<String, String> getAdditionalHudiReadOptions() {
    Map<String, String> options = new HashMap<>();
    options.put("hoodie.datasource.read.file.index.listing.mode", "eager");
    return options;
  }

  /**
   * Compares two datasets where dataset1Rows is for Iceberg and dataset2Rows is for other formats
   * (such as Delta or Hudi). - For the "uuid_field", if present, the UUID from dataset1 (Iceberg)
   * is compared with the Base64-encoded UUID from dataset2 (other formats), after decoding. - For
   * all other fields, the values are compared directly. - If neither row contains the "uuid_field",
   * the rows are compared as plain JSON strings.
   *
   * @param dataset1Rows List of JSON rows representing the dataset in Iceberg format (UUID is
   *     stored as a string).
   * @param dataset2Rows List of JSON rows representing the dataset in other formats (UUID might be
   *     Base64-encoded).
   */
  protected void compareDatasetWithUUID(List<String> dataset1Rows, List<String> dataset2Rows) {
    for (int i = 0; i < dataset1Rows.size(); i++) {
      String row1 = dataset1Rows.get(i);
      String row2 = dataset2Rows.get(i);
      if (row1.contains("uuid_field") && row2.contains("uuid_field")) {
        try {
          JsonNode node1 = OBJECT_MAPPER.readTree(row1);
          JsonNode node2 = OBJECT_MAPPER.readTree(row2);

          // check uuid field
          String uuidStr1 = node1.get("uuid_field").asText();
          byte[] bytes = Base64.getDecoder().decode(node2.get("uuid_field").asText());
          ByteBuffer bb = ByteBuffer.wrap(bytes);
          UUID uuid2 = new UUID(bb.getLong(), bb.getLong());
          String uuidStr2 = uuid2.toString();
          assertEquals(
              uuidStr1,
              uuidStr2,
              String.format(
                  "Datasets are not equivalent when reading from Spark. Source: %s, Target: %s",
                  uuidStr1, uuidStr2));

          // check other fields
          ((ObjectNode) node1).remove("uuid_field");
          ((ObjectNode) node2).remove("uuid_field");
          assertEquals(
              node1.toString(),
              node2.toString(),
              String.format(
                  "Datasets are not equivalent when comparing other fields. Source: %s, Target: %s",
                  node1, node2));
        } catch (JsonProcessingException e) {
          throw new RuntimeException(e);
        }
      } else {
        assertEquals(
            row1,
            row2,
            String.format(
                "Datasets are not equivalent when reading from Spark. Source: %s, Target: %s",
                row1, row2));
      }
    }
  }

  private static String[] getSelectColumnsArr(List<String> columnsToSelect, String format) {
    boolean isHudi = format.equals(HUDI);
    boolean isIceberg = format.equals(ICEBERG);
    return columnsToSelect.stream()
        .map(
            colName -> {
              if (colName.startsWith("timestamp_local_millis")) {
                if (isHudi) {
                  return String.format(
                      "unix_millis(CAST(%s AS TIMESTAMP)) AS %s", colName, colName);
                } else if (isIceberg) {
                  // iceberg is showing up as micros, so we need to divide by 1000 to get millis
                  return String.format("%s div 1000 AS %s", colName, colName);
                } else {
                  return colName;
                }
              } else if (isHudi && colName.startsWith("timestamp_local_micros")) {
                return String.format("unix_micros(CAST(%s AS TIMESTAMP)) AS %s", colName, colName);
              } else {
                return colName;
              }
            })
        .toArray(String[]::new);
  }

  private boolean containsUUIDFields(List<String> rows) {
    for (String row : rows) {
      if (row.contains("\"uuid_field\"")) {
        return true;
      }
    }
    return false;
  }

  protected static Stream<Arguments> addBasicPartitionCases(Stream<Arguments> arguments) {
    // add unpartitioned and partitioned cases
    return arguments.flatMap(
        args -> {
          Object[] unpartitionedArgs = Arrays.copyOf(args.get(), args.get().length + 1);
          unpartitionedArgs[unpartitionedArgs.length - 1] = PartitionConfig.of(null, null);
          Object[] partitionedArgs = Arrays.copyOf(args.get(), args.get().length + 1);
          partitionedArgs[partitionedArgs.length - 1] =
              PartitionConfig.of("level:SIMPLE", "level:VALUE");
          return Stream.of(
              Arguments.arguments(unpartitionedArgs), Arguments.arguments(partitionedArgs));
        });
  }

  protected static TableFormatPartitionDataHolder buildArgsForPartition(
      String sourceFormat,
      List<String> targetFormats,
      String hudiPartitionConfig,
      String xTablePartitionConfig,
      String filter) {
    return buildArgsForPartition(
        sourceFormat,
        targetFormats,
        hudiPartitionConfig,
        xTablePartitionConfig,
        filter,
        Collections.emptyMap());
  }

  protected static TableFormatPartitionDataHolder buildArgsForPartition(
      String sourceFormat,
      List<String> targetFormats,
      String hudiPartitionConfig,
      String xTablePartitionConfig,
      String filter,
      Map<String, String> additionalHudiReadOptions) {
    return TableFormatPartitionDataHolder.builder()
        .sourceTableFormat(sourceFormat)
        .targetTableFormats(targetFormats)
        .hudiSourceConfig(Optional.ofNullable(hudiPartitionConfig))
        .xTablePartitionConfig(xTablePartitionConfig)
        .filter(filter)
        .additionalHudiReadOptions(additionalHudiReadOptions)
        .build();
  }

  @Builder
  @Value
  protected static class TableFormatPartitionDataHolder {
    String sourceTableFormat;
    Map<String, String> sourceTableOptions;
    List<String> targetTableFormats;
    String xTablePartitionConfig;
    Optional<String> hudiSourceConfig;
    String filter;
    Map<String, String> additionalHudiReadOptions;
  }

  protected static ConversionConfig getTableSyncConfig(
      String sourceTableFormat,
      SyncMode syncMode,
      String tableName,
      GenericTable table,
      List<String> targetTableFormats,
      String partitionConfig,
      Duration metadataRetention) {
    Properties sourceProperties = new Properties();
    if (partitionConfig != null) {
      sourceProperties.put(PARTITION_FIELD_SPEC_CONFIG, partitionConfig);
    }
    SourceTable sourceTable =
        SourceTable.builder()
            .name(tableName)
            .formatName(sourceTableFormat)
            .basePath(table.getBasePath())
            .dataPath(table.getDataPath())
            .additionalProperties(sourceProperties)
            .build();

    List<TargetTable> targetTables =
        targetTableFormats.stream()
            .map(
                formatName ->
                    TargetTable.builder()
                        .name(tableName)
                        .formatName(formatName)
                        // set the metadata path to the data path as the default (required by Hudi)
                        .basePath(table.getDataPath())
                        .metadataRetention(metadataRetention)
                        .additionalProperties(new TypedProperties())
                        .build())
            .collect(Collectors.toList());

    return ConversionConfig.builder()
        .sourceTable(sourceTable)
        .targetTables(targetTables)
        .syncMode(syncMode)
        .build();
  }
}
