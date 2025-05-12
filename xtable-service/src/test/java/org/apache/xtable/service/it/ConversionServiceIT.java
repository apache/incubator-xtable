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

package org.apache.xtable.service.it;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.SparkSession;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.service.ConversionService;
import org.apache.xtable.service.models.ConvertTableRequest;
import org.apache.xtable.service.models.ConvertTableResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import jakarta.inject.Inject;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for ConversionService.
 * This test creates an actual Delta table and converts it to Iceberg format.
 */
@TestProfile(ConversionTestProfile.class)
@QuarkusTest
public class ConversionServiceIT {

    @Inject
    ConversionService conversionService;
    private Path tempDir;
    private SparkSession sparkSession;
    private String deltaTablePath;

    @BeforeEach
    public void setUp() throws Exception {
        // local fs setup
        tempDir = Files.createTempDirectory("xtable-it");

        String tableName = "xtable-service-test-" + UUID.randomUUID();
        Path basePath = tempDir.resolve(tableName);
        Files.createDirectories(basePath);

        // Setup local spark session
        sparkSession = SparkSession.builder()
                .config(getSparkConf(basePath))
                .getOrCreate();

        // Create local delta table
        deltaTablePath = basePath.resolve("delta-table").toString();
        createDeltaTable(deltaTablePath);
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (sparkSession != null) {
            sparkSession.close();
        }
        Files.walk(tempDir)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
    }

    @Test
    public void testConvertDeltaToIceberg() {
        // Create the conversion request
        ConvertTableRequest request = ConvertTableRequest.builder()
            .sourceTablePath(deltaTablePath)
            .sourceTableName("test_delta_table")
            .sourceFormat(TableFormat.DELTA)
            .targetFormats(Arrays.asList(TableFormat.ICEBERG))
            .build();

        // Execute the conversion
        ConvertTableResponse response = conversionService.convertTable(request);

        // Verify the response
        assertNotNull(response, "Response should not be null");
        assertEquals(1, response.getConvertedTables().size(), "Should have one converted table");
        assertEquals("ICEBERG", response.getConvertedTables().get(0).getTargetFormat(), "Target format should be ICEBERG");

        String metadataPath = response.getConvertedTables().get(0).getTargetMetadataPath();
        String schema = response.getConvertedTables().get(0).getTargetSchema();

        assertNotNull(metadataPath, "Metadata path should not be null");
        assertNotNull(schema, "Schema should not be null");

        Dataset<Row> icebergTable = sparkSession.read().format("iceberg").load(metadataPath);
        assertNotNull(icebergTable, "Should be able to read the Iceberg table");

        Dataset<Row> deltaTable = sparkSession.read().format("delta").load(deltaTablePath);
        assertEquals(deltaTable.count(), icebergTable.count(), "Row count should match between Delta and Iceberg");
    }

    private void createDeltaTable(String path) {
        Dataset<Row> data = sparkSession.range(0, 10).toDF("id");
        data = data.withColumn("name", org.apache.spark.sql.functions.concat(
                org.apache.spark.sql.functions.lit("name_"),
                data.col("id").cast("string")));

        data.write().format("delta").mode("overwrite").save(path);

        assertTrue(new File(path).exists(), "Delta table directory should exist");
        assertTrue(new File(path, "_delta_log").exists(), "Delta log directory should exist");
    }

    public static SparkConf getSparkConf(Path tempDir) {
    return new SparkConf()
        .setAppName("xtable-testing")
        .set("spark.serializer", KryoSerializer.class.getName())
        .set("spark.sql.catalog.default_iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .set("spark.sql.catalog.default_iceberg.type", "hadoop")
        .set("spark.sql.catalog.default_iceberg.warehouse", tempDir.toString())
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("parquet.avro.write-old-list-structure", "false")
        // Needed for ignoring not nullable constraints on nested columns in Delta.
        .set("spark.databricks.delta.constraints.allowUnenforcedNotNull.enabled", "true")
        .set("spark.sql.shuffle.partitions", "1")
        .set("spark.default.parallelism", "1")
        .set("spark.sql.session.timeZone", "UTC")
        .set(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension, org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .set("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .setMaster("local[4]");
    }
}
