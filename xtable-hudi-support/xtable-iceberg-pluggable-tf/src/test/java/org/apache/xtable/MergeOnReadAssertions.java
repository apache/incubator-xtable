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

import static org.apache.hudi.hadoop.fs.HadoopFSUtils.getStorageConf;
import static org.apache.xtable.IcebergTableAssertions.icebergTable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.SneakyThrows;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.apache.hudi.client.HoodieReadClient;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.core.io.storage.HoodieIOFactory;
import org.apache.hudi.metadata.FileSystemBackedTableMetadata;
import org.apache.hudi.storage.StoragePath;

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.CloseableIterable;

import org.apache.xtable.hudi.HudiTestUtil;

/**
 * Compares a merge-on-read table as Iceberg reads it with the table as Hudi reads it. Hudi is the
 * oracle: the rows of its snapshot read, and for every latest file slice the base file and the keys
 * its log files delete from it. The Iceberg side is read the way an Iceberg engine reads it, so a
 * deletion vector with a wrong position shows up as a different deleted key.
 */
final class MergeOnReadAssertions {
  static final String UPDATES_AS_DELETES_AND_INSERTS =
      "hoodie.write.updates.as.deletes.and.inserts";

  /** Columns of the table; Hudi's meta columns are not part of the Iceberg schema. */
  private static final String[] COMPARED_COLUMNS = {
    "key", "ts", "level", "severity", "long_field", "string_field"
  };

  private MergeOnReadAssertions() {}

  static SparkSession sparkSession(Path tempDir) {
    SparkConf sparkConf = HudiTestUtil.getSparkConf(tempDir);
    return SparkSession.builder()
        .config(HoodieReadClient.addHoodieSupport(sparkConf))
        .getOrCreate();
  }

  /**
   * Properties of a merge-on-read table whose writer keeps its log files to positional deletes.
   *
   * @param tableVersion the Hudi table version the table is created and written at
   */
  static Properties deletionVectorTable(int tableVersion) {
    Properties properties = new Properties();
    properties.setProperty(HoodieTableConfig.VERSION.key(), String.valueOf(tableVersion));
    properties.setProperty(UPDATES_AS_DELETES_AND_INSERTS, "true");
    properties.setProperty("hoodie.index.type", "SIMPLE");
    return properties;
  }

  /**
   * Rows, live files and deleted keys are the same through Iceberg as through Hudi. A file without
   * a row that is not deleted is left out on both sides: a delete of all its rows removes it from
   * Iceberg, while an update of all its rows leaves it with a deletion vector of every row.
   */
  static void assertIcebergMatchesHudi(SparkSession spark, String basePath) {
    assertEquals(hudiRows(spark, basePath), icebergRows(spark, basePath), "rows read");
    assertEquals(
        withLiveRows(deletedKeysByBaseFileInHudi(basePath)),
        withLiveRows(deletedKeysByDataFileInIceberg(spark, basePath)),
        "live files with the keys deleted from each");
  }

  /** The files among the keys of the map that hold a row whose key is not deleted. */
  static Map<String, Set<String>> withLiveRows(Map<String, Set<String>> deletedKeysByFile) {
    Map<String, Set<String>> withLiveRows = new TreeMap<>();
    deletedKeysByFile.forEach(
        (file, keys) -> {
          if (keys.size() < rowCount(file)) {
            withLiveRows.put(file, keys);
          }
        });
    return withLiveRows;
  }

  @SneakyThrows
  static long rowCount(String localPath) {
    try (ParquetFileReader reader =
        ParquetFileReader.open(
            HadoopInputFile.fromPath(
                new org.apache.hadoop.fs.Path(localPath), new Configuration()))) {
      return reader.getRecordCount();
    }
  }

  static List<String> hudiRows(SparkSession spark, String basePath) {
    return rows(spark, "hudi", basePath);
  }

  static List<String> icebergRows(SparkSession spark, String basePath) {
    return rows(spark, "iceberg", basePath);
  }

  private static List<String> rows(SparkSession spark, String format, String basePath) {
    return spark
        .read()
        .format(format)
        .load(basePath)
        .selectExpr(COMPARED_COLUMNS)
        .collectAsList()
        .stream()
        .map(row -> row.mkString("|"))
        .sorted()
        .collect(Collectors.toList());
  }

  /**
   * For every data file Iceberg plans, the record keys at the positions its deletion vector marks,
   * read back from the file itself. Also checks what Iceberg requires of the table: every planned
   * file and deletion vector file exists, a file has at most one deletion vector, and the table
   * metadata holds no deletion vector other than the planned ones.
   */
  @SneakyThrows
  static Map<String, Set<String>> deletedKeysByDataFileInIceberg(
      SparkSession spark, String basePath) {
    Table table = icebergTable(basePath);
    Map<String, Set<String>> deletedKeys = new TreeMap<>();
    Set<String> deletionVectors = new HashSet<>();
    BaseDeleteLoader deleteLoader =
        new BaseDeleteLoader(deleteFile -> table.io().newInputFile(deleteFile.location()));
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        String dataFile = task.file().location();
        String localPath = new StoragePath(dataFile).toUri().getPath();
        assertTrue(Files.exists(Paths.get(localPath)), "Iceberg references a missing " + dataFile);
        assertTrue(task.deletes().size() <= 1, "more than one delete file for " + dataFile);
        Set<String> keys = new TreeSet<>();
        for (DeleteFile deleteFile : task.deletes()) {
          assertEquals(FileFormat.PUFFIN, deleteFile.format(), "deletes must be deletion vectors");
          assertEquals(dataFile, deleteFile.referencedDataFile());
          assertTrue(
              Files.exists(Paths.get(new StoragePath(deleteFile.location()).toUri().getPath())),
              "Iceberg references a missing deletion vector file " + deleteFile.location());
          deletionVectors.add(deleteFile.location() + "#" + deleteFile.contentOffset());
          PositionDeleteIndex positions =
              deleteLoader.loadPositionDeletes(task.deletes(), dataFile);
          List<String> keysByPosition = recordKeysByPosition(spark, dataFile);
          assertEquals(deleteFile.recordCount(), positions.cardinality());
          positions.forEach(
              position -> {
                assertTrue(
                    position < keysByPosition.size(),
                    "position " + position + " is past the end of " + dataFile);
                keys.add(keysByPosition.get((int) position));
              });
          assertEquals(positions.cardinality(), keys.size(), "deleted keys of " + dataFile);
        }
        deletedKeys.put(localPath, keys);
      }
    }
    assertEquals(
        String.valueOf(deletionVectors.size()),
        table.currentSnapshot().summary().getOrDefault("total-delete-files", "0"),
        "the table metadata must hold no deletion vector of a file that is not live");
    return deletedKeys;
  }

  /** The record keys of a base file by row position, as Spark's Parquet reader numbers the rows. */
  private static List<String> recordKeysByPosition(SparkSession spark, String baseFile) {
    List<Row> rows =
        spark
            .read()
            .parquet(baseFile)
            .selectExpr("_metadata.row_index", HoodieRecord.RECORD_KEY_METADATA_FIELD)
            .collectAsList();
    String[] keys = new String[rows.size()];
    rows.forEach(row -> keys[(int) row.getLong(0)] = row.getString(1));
    return Arrays.asList(keys);
  }

  /**
   * For the base file of every file slice a Hudi snapshot reader merges, the record keys the
   * slice's log files delete from it.
   */
  @SneakyThrows
  static Map<String, Set<String>> deletedKeysByBaseFileInHudi(String basePath) {
    Configuration conf = new Configuration();
    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder()
            .setConf(getStorageConf(conf))
            .setBasePath(basePath)
            .setLoadActiveTimelineOnLoad(true)
            .build();
    HoodieLocalEngineContext engineContext = new HoodieLocalEngineContext(getStorageConf(conf));
    Map<String, Set<String>> deletedKeys = new TreeMap<>();
    if (!metaClient.getActiveTimeline().lastInstant().isPresent()) {
      return deletedKeys;
    }
    String latestInstant = metaClient.getActiveTimeline().lastInstant().get().requestedTime();
    try (HoodieTableFileSystemView fsView =
        new HoodieTableFileSystemView(
            new FileSystemBackedTableMetadata(
                engineContext, metaClient.getTableConfig(), metaClient.getStorage(), basePath),
            metaClient,
            // The instants a reader's view is built on: an instant Iceberg has not recorded is
            // pending, and its files are not part of the table.
            metaClient.getActiveTimeline().filterCompletedOrMajorOrMinorCompactionInstants())) {
      List<String> partitions =
          FSUtils.getAllPartitionPaths(
              engineContext, metaClient, HoodieMetadataConfig.newBuilder().enable(false).build());
      for (String partition : partitions) {
        List<FileSlice> slices =
            fsView
                .getLatestMergedFileSlicesBeforeOrOn(partition, latestInstant)
                .collect(Collectors.toList());
        for (FileSlice slice : slices) {
          assertTrue(slice.getBaseFile().isPresent(), "file slice without a base file: " + slice);
          String baseFile = slice.getBaseFile().get().getPath();
          Set<String> deleted = new HashSet<>();
          for (HoodieLogFile logFile : slice.getLogFiles().collect(Collectors.toList())) {
            deleted.addAll(keysDeletedBy(metaClient, logFile));
          }
          Set<String> keysInBaseFile = new HashSet<>(recordKeys(metaClient, baseFile));
          deleted.retainAll(keysInBaseFile);
          deletedKeys.put(new StoragePath(baseFile).toUri().getPath(), new TreeSet<>(deleted));
        }
      }
    }
    return deletedKeys;
  }

  @SneakyThrows
  private static List<String> keysDeletedBy(HoodieTableMetaClient metaClient, HoodieLogFile log) {
    List<String> keys = new ArrayList<>();
    try (HoodieLogFormat.Reader reader =
        HoodieLogFormat.newReader(
            metaClient, log, new TableSchemaResolver(metaClient).getTableSchema())) {
      while (reader.hasNext()) {
        HoodieLogBlock block = reader.next();
        assertTrue(block instanceof HoodieDeleteBlock, "log file with records: " + log);
        for (DeleteRecord delete : ((HoodieDeleteBlock) block).getRecordsToDelete()) {
          keys.add(delete.getRecordKey());
        }
      }
    }
    return keys;
  }

  @SneakyThrows
  private static List<String> recordKeys(HoodieTableMetaClient metaClient, String baseFile) {
    List<String> keys = new ArrayList<>();
    try (ClosableIterator<HoodieKey> iterator =
        HoodieIOFactory.getIOFactory(metaClient.getStorage())
            .getFileFormatUtils(new StoragePath(baseFile))
            .getHoodieKeyIterator(metaClient.getStorage(), new StoragePath(baseFile))) {
      iterator.forEachRemaining(key -> keys.add(key.getRecordKey()));
    }
    return keys;
  }

  /** The deletion vector files in the table's directory tree. */
  @SneakyThrows
  static Set<String> deletionVectorFiles(String basePath) {
    try (Stream<Path> files = Files.walk(Paths.get(new StoragePath(basePath).toUri().getPath()))) {
      return files
          .map(Path::toString)
          .filter(name -> name.endsWith(".puffin"))
          .collect(Collectors.toCollection(TreeSet::new));
    }
  }

  /** Paths of the deletion vector files the current Iceberg snapshot applies. */
  @SneakyThrows
  static Set<String> liveDeletionVectorFiles(String basePath) {
    Set<String> live = new TreeSet<>();
    try (CloseableIterable<FileScanTask> tasks = icebergTable(basePath).newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        task.deletes()
            .forEach(
                deleteFile -> live.add(new StoragePath(deleteFile.location()).toUri().getPath()));
      }
    }
    return live;
  }

  static long deletedRowCount(Map<String, Set<String>> deletedKeysByFile) {
    return deletedKeysByFile.values().stream().mapToLong(Set::size).sum();
  }

  static Map<String, Set<String>> filesWithDeletes(Map<String, Set<String>> deletedKeysByFile) {
    Map<String, Set<String>> withDeletes = new HashMap<>();
    deletedKeysByFile.forEach(
        (file, keys) -> {
          if (!keys.isEmpty()) {
            withDeletes.put(file, keys);
          }
        });
    return withDeletes;
  }
}
