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

import java.io.IOException;
import java.util.*;

import lombok.Builder;
import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;

/**
 * Manages Parquet file operations including reading, writing, and partition discovery and path
 * construction.
 *
 * <p>This class provides functions to handle Parquet metadata, validate schemas during appends, and
 * calculate target partition directories based on file modification times and defined partition
 * fields.
 */
@Log4j2
@Builder
public class ParquetDataManager {
  private ParquetMetadataExtractor metadataExtractor = ParquetMetadataExtractor.getInstance();
  private static final long DEFAULT_BLOCK_SIZE = 128 * 1024 * 1024; // 128MB
  private static final int DEFAULT_PAGE_SIZE = 1 * 1024 * 1024; // 1MB

  /* Use Parquet API to append to a file */

  // after appending check required before appending the file
  private static boolean checkIfSchemaIsSame(
      Configuration conf, Path fileToAppend, Path fileFromTable) {
    ParquetFileConfig schemaFileAppend = getParquetFileConfig(conf, fileToAppend);
    ParquetFileConfig schemaFileFromTable = getParquetFileConfig(conf, fileFromTable);
    return schemaFileAppend.getSchema().equals(schemaFileFromTable.getSchema());
  }

  private static ParquetFileConfig getParquetFileConfig(Configuration conf, Path fileToAppend) {
    ParquetFileConfig parquetFileConfig = new ParquetFileConfig(conf, fileToAppend);
    return parquetFileConfig;
  }
  // TODO use this method to match the partition folders and merge them one by one (if one exists
  // but not in other file then create it)
  public void appendWithPartitionCheck(Path targetPath, Path sourcePath, MessageType schema)
      throws IOException {

    // e.g., "year=2024/month=12"
    String targetPartition = getPartitionPath(targetPath);
    String sourcePartition = getPartitionPath(sourcePath);

    if (!targetPartition.equals(sourcePartition)) {
      throw new IllegalArgumentException(
          "Partition Mismatch! Cannot merge " + sourcePartition + " into " + targetPartition);
    }

    // append files within the same partition foldr
    appendNewParquetFiles(targetPath, sourcePath, schema);
  }

  private String getPartitionPath(Path path) {
    return path.getParent().getName();
  }
  // append a file (merges two files into one .parquet under a partition folder)
  public static Path appendNewParquetFiles(Path filePath, Path fileToAppend, MessageType schema)
      throws IOException {
    Configuration conf = new Configuration();
    long firstBlockIndex = getParquetFileConfig(conf, filePath).getRowGroupIndex();
    ParquetMetadata existingFooter = ParquetFileReader.readFooter(conf, filePath);
    Map<String, String> existingMeta = existingFooter.getFileMetaData().getKeyValueMetaData();
    Path tempPath = new Path(filePath.getParent(), "." + filePath.getName() + ".tmp");
    ParquetFileWriter writer =
        new ParquetFileWriter(
            HadoopOutputFile.fromPath(tempPath, conf),
            schema,
            ParquetFileWriter.Mode.OVERWRITE,
            DEFAULT_BLOCK_SIZE,
            0);
    // write the initial table with the appended file to add into the outputPath
    writer.start();
    HadoopInputFile inputFile = HadoopInputFile.fromPath(filePath, conf);
    InputFile inputFileToAppend = HadoopInputFile.fromPath(fileToAppend, conf);
    if (checkIfSchemaIsSame(conf, fileToAppend, filePath)) {
      writer.appendFile(inputFile);
      writer.appendFile(inputFileToAppend);
    }
    Map<String, String> combinedMeta = new HashMap<>();
    if (existingMeta != null) {
      combinedMeta.putAll(existingMeta);
    }
    // track the append date and save it in the footer
    int appendCount = Integer.parseInt(combinedMeta.getOrDefault("total_appends", "0"));
    // get the equivalent fileStatus from the file-to-append Path
    FileSystem fs = FileSystem.get(conf);
    FileStatus fileStatus = fs.getFileStatus(fileToAppend);
    // save block indexes and modification time (for later sync related retrieval) in the metadata
    // of the output
    // table
    int currentAppendIdx = appendCount + 1;
    long startBlock = firstBlockIndex;
    long blocksAdded = ParquetFileReader.readFooter(conf, fileToAppend).getBlocks().size();
    long endBlock = startBlock + blocksAdded - 1;

    combinedMeta.put("total_appends", String.valueOf(currentAppendIdx));
    combinedMeta.put("index_start_block_of_append_" + currentAppendIdx, String.valueOf(startBlock));
    combinedMeta.put("index_end_block_of_append_" + currentAppendIdx, String.valueOf(endBlock));
    combinedMeta.put(
        "append_date_" + currentAppendIdx, String.valueOf(fileStatus.getModificationTime()));
    writer.end(combinedMeta);
    fs.delete(filePath, false);
    fs.rename(tempPath, filePath);
    return filePath;
  }
  // selective compaction of parquet blocks
  public static List<Path> formNewTargetFiles(
      Configuration conf, Path partitionPath, long targetModifTime) throws IOException {
    List<Path> finalPaths = new ArrayList<>();
    FileSystem fs = partitionPath.getFileSystem(conf);

    FileStatus[] statuses =
        fs.listStatus(partitionPath, path -> path.getName().endsWith(".parquet"));

    for (FileStatus status : statuses) {

      Path filePath = status.getPath();
      ParquetMetadata bigFileFooter =
          ParquetFileReader.readFooter(conf, filePath, ParquetMetadataConverter.NO_FILTER);
      MessageType schema = bigFileFooter.getFileMetaData().getSchema();
      int totalAppends =
          Integer.parseInt(
              bigFileFooter
                  .getFileMetaData()
                  .getKeyValueMetaData()
                  .getOrDefault("total_appends", "0"));
      for (int i = 1; i <= totalAppends; i++) {
        int startBlock =
            Integer.valueOf(
                bigFileFooter
                    .getFileMetaData()
                    .getKeyValueMetaData()
                    .get("index_start_block_of_append_" + i));
        int endBlock =
            Integer.valueOf(
                bigFileFooter
                    .getFileMetaData()
                    .getKeyValueMetaData()
                    .get("index_end_block_of_append_" + i));
        String newFileName =
            String.format(
                "%s_block%d_%d.parquet", status.getPath().getName(), startBlock, endBlock);
        Path targetSyncFilePath = new Path(status.getPath().getParent(), newFileName);
        try (ParquetFileWriter writer = new ParquetFileWriter(conf, schema, targetSyncFilePath)) {
          writer.start();
          for (int j = 0; j < bigFileFooter.getBlocks().size(); j++) {
            BlockMetaData blockMetadata = bigFileFooter.getBlocks().get(j);
            if (j >= startBlock
                && j <= endBlock
                && status.getModificationTime() >= targetModifTime) {
              List<BlockMetaData> blockList =
                  Collections.<BlockMetaData>singletonList(blockMetadata);
              FSDataInputStream targetInputStream = fs.open(status.getPath());
              writer.appendRowGroups(targetInputStream, blockList, false);
            }
          }
          writer.end(new HashMap<>());
          finalPaths.add(targetSyncFilePath);
        }
      }
    }
    return finalPaths;
  }

  public static Path appendPartitionedData(Path targetPath, Path sourcePath, MessageType schema)
      throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = sourcePath.getFileSystem(conf);

    List<Path> targetFiles = collectParquetFiles(fs, targetPath);

    List<Path> sourceFiles = collectParquetFiles(fs, sourcePath);

    if (targetFiles.isEmpty()) {
      throw new IOException("Target directory contains no parquet files to append to.");
    }
    if (sourceFiles.isEmpty()) {
      return targetPath;
    }
    Path masterTargetFile = null;
    for (Path p : targetFiles) {
      ParquetMetadata metadata = ParquetFileReader.readFooter(conf, p);
      if (!metadata.getBlocks().isEmpty()) {
        masterTargetFile = p;
        break;
      }
    }

    if (masterTargetFile == null) {
      throw new IOException("Target directory contains no files with valid data blocks.");
    }

    for (int i = 1; i < targetFiles.size(); i++) {
      long len = fs.getFileStatus(targetFiles.get(i)).getLen();
      System.out.println("DEBUG: Attempting to append " + targetFiles.get(i) + " Size: " + len);
      appendNewParquetFiles(masterTargetFile, targetFiles.get(i), schema);
    }
    for (Path sourceFile : sourceFiles) {
      ParquetMetadata sourceFooter = ParquetFileReader.readFooter(conf, sourceFile);
      if (sourceFooter.getBlocks().isEmpty()) {
        System.out.println("SKIPPING: " + sourceFile + " has no data.");
        continue;
      }
      long len = fs.getFileStatus(sourceFile).getLen();
      System.out.println("DEBUG: Attempting to append " + sourceFile + " Size: " + len);
      appendNewParquetFiles(masterTargetFile, sourceFile, schema);
    }

    return masterTargetFile;
  }

  private static List<Path> collectParquetFiles(FileSystem fs, Path root) throws IOException {
    List<Path> parquetFiles = new ArrayList<>();
    Configuration conf = fs.getConf();
    if (!fs.exists(root)) return parquetFiles;

    if (fs.getFileStatus(root).isDirectory()) {
      RemoteIterator<LocatedFileStatus> it = fs.listFiles(root, true);
      while (it.hasNext()) {
        Path p = it.next().getPath();
        if (p.getName().endsWith(".parquet")
            && !p.toString().contains("/.")
            && !p.toString().contains("/_")) {
          ParquetMetadata footer = ParquetFileReader.readFooter(conf, p);
          if (footer.getBlocks() != null && !footer.getBlocks().isEmpty()) {
            parquetFiles.add(p);
          }
        }
      }
    } else {
      parquetFiles.add(root);
    }
    return parquetFiles;
  }

  public static List<Path> mergeDatasetsByPartition(
      Path targetRoot, Path sourceRoot, MessageType schema) throws IOException {
    List<Path> finalPartitionPaths = new ArrayList<>();
    Configuration conf = new Configuration();
    FileSystem fs = targetRoot.getFileSystem(conf);

    Map<String, Path> sourcePartitions = mapPartitions(fs, sourceRoot);

    Map<String, Path> targetPartitions = mapPartitions(fs, targetRoot);

    for (Map.Entry<String, Path> entry : sourcePartitions.entrySet()) {
      String partitionKey = entry.getKey();
      Path sourcePartitionPath = entry.getValue();

      if (targetPartitions.containsKey(partitionKey)) {
        Path targetPartitionPath = targetPartitions.get(partitionKey);

        finalPartitionPaths.add(
            appendPartitionedData(targetPartitionPath, sourcePartitionPath, schema));
      } else {
        // TODO logic for when a partition exists in source but not in target
        // simply merge the files in the respective partitions using appendPartitionedData() that
        // takes only one Path/dir

      }
    }
    return finalPartitionPaths;
  }

  private static Map<String, Path> mapPartitions(FileSystem fs, Path root) throws IOException {
    Map<String, Path> partitionMap = new HashMap<>();
    RemoteIterator<LocatedFileStatus> it = fs.listFiles(root, true);

    while (it.hasNext()) {
      Path filePath = it.next().getPath();
      if (filePath.getName().endsWith(".parquet") && !filePath.toString().contains("/_")) {
        Path parent = filePath.getParent();
        String relative = parent.toString().replace(root.toString(), "");
        if (relative.startsWith("/")) relative = relative.substring(1);

        partitionMap.put(relative, parent);
      }
    }
    return partitionMap;
  }
}
