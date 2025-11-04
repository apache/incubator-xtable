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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import lombok.SneakyThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.hudi.common.model.HoodieTableType;

import org.apache.xtable.GenericTable;
import org.apache.xtable.TestJavaHudiTable;

class ITRunSync {

  @Test
  void testSingleSyncMode(@TempDir Path tempDir) throws IOException {
    String tableName = "test-table";
    try (GenericTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, null, HoodieTableType.COPY_ON_WRITE)) {
      table.insertRows(20);
      File configFile = writeConfigFile(tempDir, table, tableName);
      String[] args = new String[] {"--datasetConfig", configFile.getPath()};
      RunSync.main(args);
      Path icebergMetadataPath = Paths.get(URI.create(table.getBasePath() + "/metadata"));
      waitForNumIcebergCommits(icebergMetadataPath, 3);
    }
  }

  @Test
  void testContinuousSyncMode(@TempDir Path tempDir) throws IOException {
    ExecutorService runner = Executors.newSingleThreadExecutor();
    String tableName = "test-table";
    try (GenericTable table =
        TestJavaHudiTable.forStandardSchemaWithFieldIds(
            tableName, tempDir, null, HoodieTableType.COPY_ON_WRITE)) {
      table.insertRows(20);
      File configFile = writeConfigFile(tempDir, table, tableName);
      String[] args = new String[] {"--datasetConfig", configFile.getPath(), "--continuousMode"};
      runner.submit(
          () -> {
            try {
              RunSync.main(args);
            } catch (IOException ex) {
              throw new UncheckedIOException(ex);
            }
          });
      Path icebergMetadataPath = Paths.get(URI.create(table.getBasePath() + "/metadata"));
      waitForNumIcebergCommits(icebergMetadataPath, 3);
    }
    try (GenericTable table =
        TestJavaHudiTable.withAdditionalColumnsAndFieldIds(
            tableName, tempDir, null, HoodieTableType.COPY_ON_WRITE)) {
      // write more data now that table is initialized and data is synced
      table.insertRows(20);
      Path icebergMetadataPath = Paths.get(URI.create(table.getBasePath() + "/metadata"));
      waitForNumIcebergCommits(icebergMetadataPath, 6);
      assertEquals(6, numIcebergMetadataJsonFiles(icebergMetadataPath));
    } finally {
      runner.shutdownNow();
    }
  }

  private static File writeConfigFile(Path tempDir, GenericTable table, String tableName)
      throws IOException {
    RunSync.DatasetConfig config =
        RunSync.DatasetConfig.builder()
            .sourceFormat("HUDI")
            .targetFormats(Collections.singletonList("ICEBERG"))
            .datasets(
                Collections.singletonList(
                    RunSync.DatasetConfig.Table.builder()
                        .tableBasePath(table.getBasePath())
                        .tableName(tableName)
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
