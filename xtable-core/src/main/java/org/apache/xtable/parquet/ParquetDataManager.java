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
  private boolean checkIfSchemaIsSame(Configuration conf, Path fileToAppend, Path fileFromTable) {
    ParquetFileConfig schemaFileAppend = getParquetFileConfig(conf, fileToAppend);
    ParquetFileConfig schemaFileFromTable = getParquetFileConfig(conf, fileFromTable);
    return schemaFileAppend.getSchema().equals(schemaFileFromTable.getSchema());
  }

  private ParquetFileConfig getParquetFileConfig(Configuration conf, Path fileToAppend) {
    ParquetFileConfig parquetFileConfig = new ParquetFileConfig(conf, fileToAppend);
    return parquetFileConfig;
  }

  // append a file into a table (merges two files into one .parquet under a partition folder)
  public void appendNewParquetFiles(Path filePath, Path fileToAppend, MessageType schema)
      throws IOException {
    Configuration conf = new Configuration();
    long firstBlockIndex = getParquetFileConfig(conf, fileToAppend).getRowGroupSize();
    ParquetFileWriter writer =
        new ParquetFileWriter(
            HadoopOutputFile.fromPath(filePath, conf),
            // HadoopOutputFile.fromPath(outputPath, conf),
            schema,
            ParquetFileWriter.Mode.CREATE,
            DEFAULT_BLOCK_SIZE,
            0);
    // write the initial table with the appended file to add into the outputPath
    writer.start();
    // HadoopInputFile inputFile = HadoopInputFile.fromPath(filePath, conf);
    InputFile inputFileToAppend = HadoopInputFile.fromPath(fileToAppend, conf);
    if (checkIfSchemaIsSame(conf, fileToAppend, filePath)) {
      // writer.appendFile(inputFile);
      writer.appendFile(inputFileToAppend);
    }
    Map<String, String> combinedMeta = new HashMap<>();
    // track the append date and save it in the footer
    int appendCount =
        Integer.parseInt(
            ParquetFileReader.readFooter(conf, filePath)
                .getFileMetaData()
                .getKeyValueMetaData()
                .getOrDefault("total_appends", "0"));
    String newKey = "append_date_" + (appendCount + 1);
    // get the equivalent fileStatus from the file-to-append Path
    FileSystem fs = FileSystem.get(conf);
    FileStatus fileStatus = fs.getFileStatus(fileToAppend);
    // save block indexes and modification time (for later sync related retrieval) in the metadata
    // of the output
    // table
    combinedMeta.put(newKey, String.valueOf(fileStatus.getModificationTime()));
    combinedMeta.put("total_appends", String.valueOf(appendCount + 1));
    combinedMeta.put(
        "index_start_block_of_append_" + String.valueOf(appendCount + 1),
        String.valueOf(firstBlockIndex));
    writer.end(combinedMeta);
  }
  // selective compaction of parquet blocks
  public List<Path> formNewTargetFiles(Configuration conf, Path directoryPath, long targetModifTime)
      throws IOException {
    List<Path> finalPaths = new ArrayList<>();
    FileSystem fs = directoryPath.getFileSystem(conf);

    FileStatus[] statuses =
        fs.listStatus(directoryPath, path -> path.getName().endsWith(".parquet"));

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
      for (int i = 1; i < totalAppends; i++) {
        int startBlock =
            Integer.valueOf(
                bigFileFooter
                    .getFileMetaData()
                    .getKeyValueMetaData()
                    .get("index_start_block_of_append_" + totalAppends));
        int endBlock =
            Integer.valueOf(
                bigFileFooter
                    .getFileMetaData()
                    .getKeyValueMetaData()
                    .get("index_end_block_of_append_" + (totalAppends + 1)));
        Path targetSyncFilePath =
            new Path(
                status.getPath().getName() + String.valueOf(startBlock) + String.valueOf(endBlock));
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
  // Find and retrieve the file paths that satisfy the time condition
  // Each partition folder contains one merged_file in turn containing many appends
  public List<Path> findFilesAfterModifTime(
      Configuration conf, Path directoryPath, long targetModifTime) throws IOException {
    List<Path> results = new ArrayList<>();
    FileSystem fs = directoryPath.getFileSystem(conf);

    FileStatus[] statuses =
        fs.listStatus(directoryPath, path -> path.getName().endsWith(".parquet"));

    for (FileStatus status : statuses) {
      Path filePath = status.getPath();

      try {
        ParquetMetadata footer = ParquetFileReader.readFooter(conf, filePath);
        Map<String, String> meta = footer.getFileMetaData().getKeyValueMetaData();

        int totalAppends = Integer.parseInt(meta.getOrDefault("total_appends", "0"));
        if (Long.parseLong(meta.get("append_date_0")) > targetModifTime) {
          results.add(filePath);
          break;
        } else if (Long.parseLong(meta.get("append_date_" + totalAppends)) < targetModifTime) {
          continue;
        }
        // OR
        /*if (status.getPath().getName().endsWith(".parquet") &&
                status.getModificationTime() > targetModifTime) {
          results.add(status.getPath());
        }*/
      } catch (Exception e) {
        log.error("Could not read metadata for: {}", filePath, e);
        throw new IOException("Could not read metadata for", e);
      }
    }

    return results;
  }
}
