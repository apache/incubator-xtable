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

import static org.apache.xtable.IcebergTableAssertions.assertNothingPending;
import static org.apache.xtable.IcebergTableAssertions.reconstructedTimeline;
import static org.apache.xtable.MergeOnReadAssertions.assertIcebergMatchesHudi;
import static org.apache.xtable.MergeOnReadAssertions.deletedKeysByDataFileInIceberg;
import static org.apache.xtable.MergeOnReadAssertions.deletedRowCount;
import static org.apache.xtable.MergeOnReadAssertions.deletionVectorTable;
import static org.apache.xtable.MergeOnReadAssertions.icebergRows;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;

/**
 * The Spark write client splits updates into deletes and inserts, routes them and compacts in its
 * own engine-specific code, so a table it writes has to be published the same way as one the Java
 * client writes.
 */
class ITIcebergSparkMergeOnReadDeletionVectors {
  @TempDir public static Path tempDir;

  private static SparkSession spark;
  private static JavaSparkContext jsc;

  @BeforeAll
  static void setupOnce() {
    spark = MergeOnReadAssertions.sparkSession(tempDir);
    jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
  }

  @AfterAll
  static void teardown() {
    if (spark != null) {
      spark.stop();
    }
  }

  @ParameterizedTest
  @CsvSource({"8,false", "9,false", "9,true", "10,false", "10,true"})
  void insertsUpdatesDeletesAndCompaction(int tableVersion, boolean partitioned) {
    try (TestSparkHudiTable table =
        TestSparkHudiTable.forStandardSchema(
            "mor_spark_" + UUID.randomUUID().toString().replace('-', '_'),
            tempDir,
            jsc,
            partitioned ? "level:SIMPLE" : null,
            HoodieTableType.MERGE_ON_READ,
            deletionVectorTable(tableVersion))) {
      String basePath = table.getBasePath();
      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(60, true);
      assertIcebergMatchesHudi(spark, basePath);

      table.upsertRecords(inserts.subList(0, 15), true);
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(15, deletedRowCount(deletedKeysByDataFileInIceberg(spark, basePath)));
      assertEquals(60, icebergRows(spark, basePath).size());

      table.upsertRecords(inserts.subList(10, 25), true);
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(60, icebergRows(spark, basePath).size());

      table.deleteRecords(inserts.subList(25, 30), true);
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(55, icebergRows(spark, basePath).size());

      table.compact();
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(0, deletedRowCount(deletedKeysByDataFileInIceberg(spark, basePath)));
      assertEquals(55, icebergRows(spark, basePath).size());

      table.deleteRecords(inserts.subList(30, 35), true);
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(50, icebergRows(spark, basePath).size());
      assertNothingPending(reconstructedTimeline(basePath));
    }
  }
}
