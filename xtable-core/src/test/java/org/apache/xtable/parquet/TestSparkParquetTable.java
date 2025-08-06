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
package org.apache.xtable.parquet;

import org.apache.avro.Schema;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.parquet.schema.Type;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.xtable.GenericTable;
import org.apache.xtable.TestSparkHudiTable;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.parquet.example.data.Group;

public class TestSparkParquetTable implements GenericTable<Group, String> {
    // Name of the table
    protected String tableName;
    // Base path for the table
    protected String basePath;
    protected Path tempDir;
    protected String partitionConfig;
    protected MessageType schema;
    protected List<String> partitionFieldNames;
    private JavaSparkContext jsc;

    private TestSparkParquetTable(
            String name,
            Path tempDir,
            JavaSparkContext jsc,
            String partitionConfig) {
        // initialize spark session
        try {
            this.tableName = name;
            this.tempDir = tempDir;
            this.partitionConfig = partitionConfig;
            this.jsc = jsc;
            this.basePath = initBasePath(tempDir, name);
        } catch (IOException ex) {
            throw new UncheckedIOException("Unable to initialize Test Parquet File", ex);
        }
    }

    public static TestSparkParquetTable forStandardSchemaAndPartitioning(
            String tableName, Path tempDir, JavaSparkContext jsc, boolean isPartitioned) {
        String partitionConfig = isPartitioned ? "level:SIMPLE" : null;
        return new TestSparkParquetTable(tableName, tempDir, jsc, partitionConfig);

    }

    protected String initBasePath(Path tempDir, String tableName) throws IOException {
        Path basePath = tempDir.resolve(tableName + "_data");
        Files.createDirectories(basePath);
        return basePath.toUri().toString();
    }

    public List<Group> insertRows(int numRows) {
        return null;
    }

    public List<Group> insertRecordsForSpecialPartition(int numRows) {
        return null;
    }

    public void upsertRows(List<Group> rows) {

    }

    public void deleteRows(List<Group> rows) {

    }

    public void deletePartition(String partitionValue) {

    }

    public void deleteSpecialPartition() {

    }

    public String getBasePath() {
        return basePath;
    }

    public String getMetadataPath() {
        return null;
    }

    public String getOrderByColumn() {
        return null;
    }

    public void close() {

    }

    public void reload() {

    }

    public List<String> getColumnsToSelect() {
        return schema.getFields().stream().map(Type::getName).collect(Collectors.toList());
    }

    public String getFilterQuery() {
        return null;
    }
}
