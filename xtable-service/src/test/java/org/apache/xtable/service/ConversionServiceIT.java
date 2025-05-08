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

package org.apache.xtable.service;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.service.models.ConvertTableRequest;
import org.apache.xtable.service.models.ConvertTableResponse;
import org.apache.xtable.service.spark.SparkHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.commons.io.FileUtils;

import jakarta.inject.Inject;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for ConversionService.
 * This test creates an actual Delta table and converts it to Iceberg format.
 */
@TestProfile(ConversionTestProfile.class)
@QuarkusTest
public class ConversionServiceIT {

    @Inject
    ConversionService conversionService;

    @Inject
    SparkHolder sparkHolder;

    private Path testBasePath;
    private String deltaTablePath;
    
    @BeforeEach
    public void setUp() throws Exception {
        // Create a unique test directory in a project-specific location
        File baseDir = new File("target/test-data");
        baseDir.mkdirs();
        testBasePath = baseDir.toPath().resolve("xtable_test_" + System.currentTimeMillis());
        Files.createDirectories(testBasePath);
        deltaTablePath = testBasePath.resolve("delta_table").toString();

        // Create a Delta table with test data
        createDeltaTable(deltaTablePath);
    }

    @AfterEach
    public void tearDown() throws Exception {
        // Clean up all test resources
        FileUtils.deleteDirectory(testBasePath.toFile());
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
        String schemaPath = response.getConvertedTables().get(0).getTargetSchema();

        assertNotNull(metadataPath, "Metadata path should not be null");
        assertNotNull(schemaPath, "Schema path should not be null");

        File metadataDir = new File(metadataPath);
        assertTrue(metadataDir.exists(), "Metadata directory should exist");

        File metadataFile = new File(metadataDir, "metadata.json");
        assertTrue(metadataFile.exists(), "metadata.json should exist in the metadata directory");

        Dataset<Row> icebergTable = sparkHolder.spark().read().format("iceberg").load(metadataPath);
        assertNotNull(icebergTable, "Should be able to read the Iceberg table");

        Dataset<Row> deltaTable = sparkHolder.spark().read().format("delta").load(deltaTablePath);
        assertEquals(deltaTable.count(), icebergTable.count(), "Row count should match between Delta and Iceberg");
    }

    private void createDeltaTable(String path) {
        Dataset<Row> data = sparkHolder.spark().range(0, 10).toDF("id");
        data = data.withColumn("name", org.apache.spark.sql.functions.concat(
                org.apache.spark.sql.functions.lit("name_"),
                data.col("id").cast("string")));

        data.write().format("delta").mode("overwrite").save(path);

        assertTrue(new File(path).exists(), "Delta table directory should exist");
        assertTrue(new File(path, "_delta_log").exists(), "Delta log directory should exist");
    }
}
