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
 
package org.apache.xtable.utilities;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import lombok.SneakyThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.hudi.common.model.HoodieTableType;

import org.apache.xtable.GenericTable;
import org.apache.xtable.TestJavaHudiTable;
import org.apache.xtable.conversion.ExternalCatalogConfig;
import org.apache.xtable.model.storage.CatalogType;
import org.apache.xtable.testutil.ITTestUtils;
import org.apache.xtable.utilities.RunCatalogSync.DatasetConfig;
import org.apache.xtable.utilities.RunCatalogSync.DatasetConfig.SourceTableIdentifier;
import org.apache.xtable.utilities.RunCatalogSync.DatasetConfig.StorageIdentifier;
import org.apache.xtable.utilities.RunCatalogSync.DatasetConfig.TableIdentifier;
import org.apache.xtable.utilities.RunCatalogSync.DatasetConfig.TargetTableIdentifier;

public class ITRunCatalogSync {

  @Test
  void testCatalogSync(@TempDir Path tempDir) throws Exception {
    String tableName = "test-table";
    try (GenericTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, null, HoodieTableType.COPY_ON_WRITE)) {
      table.insertRows(20);
      File configFile = writeConfigFile(tempDir, table, tableName);
      String[] args = new String[] {"-catalogConfig", configFile.getPath()};
      RunCatalogSync.main(args);
    }
  }

  private static File writeConfigFile(Path tempDir, GenericTable table, String tableName)
      throws IOException {
    DatasetConfig config =
        DatasetConfig.builder()
            .sourceCatalog(
                ExternalCatalogConfig.builder()
                    .catalogId("source-catalog-1")
                    .catalogType(CatalogType.STORAGE)
                    .build())
            .targetCatalogs(
                Collections.singletonList(
                    ExternalCatalogConfig.builder()
                        .catalogId("target-catalog-1")
                        .catalogSyncClientImpl(ITTestUtils.TestCatalogSyncImpl.class.getName())
                        .build()))
            .datasets(
                Collections.singletonList(
                    DatasetConfig.Dataset.builder()
                        .sourceCatalogTableIdentifier(
                            SourceTableIdentifier.builder()
                                .storageIdentifier(
                                    StorageIdentifier.builder()
                                        .tableBasePath(table.getBasePath())
                                        .tableName(tableName)
                                        .tableFormat("HUDI")
                                        .build())
                                .build())
                        .targetCatalogTableIdentifiers(
                            Arrays.asList(
                                TargetTableIdentifier.builder()
                                    .catalogId("target-catalog-1")
                                    .tableFormat("DELTA")
                                    .tableIdentifier(
                                        TableIdentifier.builder()
                                            .hierarchicalId("database-1.table-1")
                                            .build())
                                    .build(),
                                TargetTableIdentifier.builder()
                                    .catalogId("target-catalog-1")
                                    .tableFormat("ICEBERG")
                                    .tableIdentifier(
                                        TableIdentifier.builder()
                                            .hierarchicalId("catalog-2.database-2.table-2")
                                            .build())
                                    .build()))
                        .build()))
            .build();
    File configFile = new File(tempDir + "config.yaml");
    RunSync.YAML_MAPPER.writeValue(configFile, config);
    return configFile;
  }

  @SneakyThrows
  private static void waitForNumIcebergCommits(Path metadataPath, int count) {
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < TimeUnit.MINUTES.toMillis(5)) {
      if (numIcebergMetadataJsonFiles(metadataPath) == count) {
        break;
      }
      Thread.sleep(5000);
    }
  }

  @SneakyThrows
  private static long numIcebergMetadataJsonFiles(Path path) {
    long count = 0;
    if (Files.exists(path)) {
      count = Files.list(path).filter(p -> p.toString().endsWith("metadata.json")).count();
    }
    return count;
  }
}
