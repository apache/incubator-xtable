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

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;

import org.apache.hudi.client.HoodieReadClient;
import org.apache.hudi.common.config.HoodieMetadataConfig;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.apache.xtable.hudi.HudiTestUtil;
import org.apache.xtable.model.storage.TableFormat;

class ConversionTestingBase {
  @TempDir public static Path tempDir;
  protected static final DateTimeFormatter DATE_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.of("UTC"));
  protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  protected static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

  protected static JavaSparkContext jsc;
  protected static SparkSession sparkSession;

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

  protected static List<String> getOtherFormats(String sourceTableFormat) {
    return Arrays.stream(TableFormat.values())
        .filter(format -> !format.equals(sourceTableFormat))
        .collect(Collectors.toList());
  }

  protected void checkDatasetEquivalenceWithFilter(
      String sourceFormat,
      GenericTable<?, ?> sourceTable,
      List<String> targetFormats,
      String filter) {
    checkDatasetEquivalence(
        sourceFormat,
        sourceTable,
        Collections.emptyMap(),
        targetFormats,
        Collections.emptyMap(),
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
                      if (targetFormat.equals(TableFormat.HUDI)) {
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

    String[] selectColumnsArr = sourceTable.getColumnsToSelect().toArray(new String[] {});
    List<String> dataset1Rows = sourceRows.selectExpr(selectColumnsArr).toJSON().collectAsList();
    targetRowsByFormat.forEach(
        (format, targetRows) -> {
          List<String> dataset2Rows =
              targetRows.selectExpr(selectColumnsArr).toJSON().collectAsList();
          Assertions.assertEquals(
              dataset1Rows.size(),
              dataset2Rows.size(),
              String.format(
                  "Datasets have different row counts when reading from Spark. Source: %s, Target: %s",
                  sourceFormat, format));
          // sanity check the count to ensure test is set up properly
          if (expectedCount != null) {
            Assertions.assertEquals(expectedCount, dataset1Rows.size());
          } else {
            // if count is not known ahead of time, ensure datasets are non-empty
            Assertions.assertFalse(dataset1Rows.isEmpty());
          }

          if (containsUUIDFields(dataset1Rows) && containsUUIDFields(dataset2Rows)) {
            compareDatasetWithUUID(dataset1Rows, dataset2Rows);
          } else {
            Assertions.assertEquals(
                dataset1Rows,
                dataset2Rows,
                String.format(
                    "Datasets are not equivalent when reading from Spark. Source: %s, Target: %s",
                    sourceFormat, format));
          }
        });
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
  private void compareDatasetWithUUID(List<String> dataset1Rows, List<String> dataset2Rows) {
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
          Assertions.assertEquals(
              uuidStr1,
              uuidStr2,
              String.format(
                  "Datasets are not equivalent when reading from Spark. Source: %s, Target: %s",
                  uuidStr1, uuidStr2));

          // check other fields
          ((ObjectNode) node1).remove("uuid_field");
          ((ObjectNode) node2).remove("uuid_field");
          Assertions.assertEquals(
              node1.toString(),
              node2.toString(),
              String.format(
                  "Datasets are not equivalent when comparing other fields. Source: %s, Target: %s",
                  node1, node2));
        } catch (JsonProcessingException e) {
          throw new RuntimeException(e);
        }
      } else {
        Assertions.assertEquals(
            row1,
            row2,
            String.format(
                "Datasets are not equivalent when reading from Spark. Source: %s, Target: %s",
                row1, row2));
      }
    }
  }

  private boolean containsUUIDFields(List<String> rows) {
    for (String row : rows) {
      if (row.contains("\"uuid_field\"")) {
        return true;
      }
    }
    return false;
  }
}
