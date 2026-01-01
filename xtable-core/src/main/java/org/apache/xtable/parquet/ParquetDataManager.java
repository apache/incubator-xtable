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
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.Builder;
import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;

import org.apache.xtable.model.schema.InternalPartitionField;

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

  Instant parsePartitionDirToStartTime(
      String partitionDir, List<InternalPartitionField> partitionFields) {
    Map<String, String> partitionValues = new HashMap<>();
    String[] parts = partitionDir.split("/");
    for (String part : parts) {
      String[] keyValue = part.split("=");
      if (keyValue.length == 2) {
        partitionValues.put(keyValue[0].toLowerCase(), keyValue[1]);
      }
    }
    int year = 1970;
    int month = 1;
    int day = 1;
    int hour = 0;
    int minute = 0;
    int second = 0;

    for (InternalPartitionField field : partitionFields) {
      String fieldName = field.getSourceField().getName().toLowerCase();
      String value = partitionValues.get(fieldName);

      if (value != null) {
        try {
          int intValue = Integer.parseInt(value);

          switch (fieldName) {
            case "year":
              year = intValue;
              break;
            case "month":
              month = intValue;
              break;
            case "day":
              day = intValue;
              break;
            case "hour":
              hour = intValue;
              break;
            case "minute":
              minute = intValue;
              break;
            case "second":
              second = intValue;
              break;
            default:
              break;
          }
        } catch (NumberFormatException e) {
          System.err.println(
              "Warning: Invalid number format for partition field '" + fieldName + "': " + value);
        }
      }
    }

    try {
      LocalDateTime localDateTime = LocalDateTime.of(year, month, day, hour, minute, second);
      return localDateTime.toInstant(ZoneOffset.UTC);
    } catch (java.time.DateTimeException e) {
      throw new IllegalArgumentException(
          "Invalid partition directory date components: " + partitionDir, e);
    }
  }

  private ChronoUnit getGranularityUnit(List<InternalPartitionField> partitionFields) {
    if (partitionFields == null || partitionFields.isEmpty()) {
      return ChronoUnit.DAYS;
    }
    // get the most granular field (the last one in the list, e.g., 'hour' after 'year' and 'month')
    String lastFieldName =
        partitionFields.get(partitionFields.size() - 1).getSourceField().getName();

    if (lastFieldName.equalsIgnoreCase("year")) {
      return ChronoUnit.YEARS;
    } else if (lastFieldName.equalsIgnoreCase("month")) {
      return ChronoUnit.MONTHS;
    } else if (lastFieldName.equalsIgnoreCase("day") || lastFieldName.equalsIgnoreCase("date")) {
      return ChronoUnit.DAYS;
    } else if (lastFieldName.equalsIgnoreCase("hour")) {
      return ChronoUnit.HOURS;
    } else if (lastFieldName.equalsIgnoreCase("minute")) {
      return ChronoUnit.MINUTES;
    } else {
      return ChronoUnit.DAYS;
    }
  }

  // find the target partition by comparing the modficationTime of the file to add with the current
  // tables
  // if the modifTime falls between two partitions then we know it belongs to the first
  private String findTargetPartitionFolder(
      Instant modifTime,
      List<InternalPartitionField> partitionFields,
      // could be retrieved from getParquetFiles() of ParquetConversionSource
      Stream<LocatedFileStatus> parquetFiles)
      throws IOException {

    long modifTimeMillis = modifTime.toEpochMilli();
    final ZoneId ZONE = ZoneId.systemDefault();
    // find the partition granularity (days, hours...)
    ChronoUnit partitionUnit = getGranularityUnit(partitionFields);
    List<String> allFolders =
        parquetFiles.map(status -> status.getPath().toString()).collect(Collectors.toList());

    // sort the folders by their start time
    allFolders.sort(Comparator.comparing(f -> parsePartitionDirToStartTime(f, partitionFields)));

    for (int i = 0; i < allFolders.size(); i++) {
      String currentFolder = allFolders.get(i);

      // start time of the current folder
      Instant currentStartTime = parsePartitionDirToStartTime(currentFolder, partitionFields);
      long currentStartMillis = currentStartTime.toEpochMilli();

      // determine the end time (which is the start time of the next logical partition)
      long nextStartMillis;
      if (i + 1 < allFolders.size()) {
        nextStartMillis =
            parsePartitionDirToStartTime(allFolders.get(i + 1), partitionFields).toEpochMilli();
      } else {
        ZonedDateTime currentStartZDT = currentStartTime.atZone(ZONE);
        ZonedDateTime nextStartZDT = currentStartZDT.plus(1, partitionUnit);
        // to evaluate the partition date value and make comparable with the modifTime in Instant
        nextStartMillis = nextStartZDT.toInstant().toEpochMilli();
      }
      if (modifTimeMillis >= currentStartMillis && modifTimeMillis < nextStartMillis) {
        return currentFolder;
      }
    }
    return "";
  }

  private Path writeNewParquetFile(
      Configuration conf, ParquetReader reader, Path fileToAppend, Path outputFile)
      throws IOException {
    ParquetFileConfig parquetFileConfig = getParquetFileConfig(conf, fileToAppend);
    int pageSize = ParquetWriter.DEFAULT_PAGE_SIZE;
    try (ParquetWriter<Group> writer =
        new ParquetWriter<Group>(
            outputFile,
            new GroupWriteSupport(),
            parquetFileConfig.getCodec(),
            (int) parquetFileConfig.getRowGroupSize(),
            pageSize,
            pageSize, // dictionaryPageSize
            true, // enableDictionary
            false, // enableValidation
            ParquetWriter.DEFAULT_WRITER_VERSION,
            conf)) {
      Group currentGroup = null;
      while ((currentGroup = (Group) reader.read()) != null) {
        writer.write(currentGroup);
      }
    } catch (Exception e) {
      log.error("Unexpected error during Parquet write: {}", outputFile, e);
      throw new IOException("Failed to complete Parquet write operation", e);
    }
    return outputFile;
  }
  // partition fields are already computed, given a parquet file InternalDataFile must be derived
  // (e.g., using createInternalDataFileFromParquetFile())
  private void appendNewParquetFile(
      Configuration conf,
      String rootPath,
      FileStatus parquetFile,
      Stream<LocatedFileStatus> parquetFiles,
      List<InternalPartitionField> partitionFields)
      throws IOException {
    Path finalFile = null;
    String partitionDir = "";
    Instant modifTime = Instant.ofEpochMilli(parquetFile.getModificationTime());
    Path fileToAppend = parquetFile.getPath();
    // construct the file path to inject into the existing partitioned file
    if (partitionFields == null || partitionFields.isEmpty()) {
      String partitionValue =
          DateTimeFormatter.ISO_LOCAL_DATE.format(
              modifTime.atZone(ZoneId.systemDefault()).toLocalDate());
      partitionDir = partitionFields.get(0).getSourceField().getName() + partitionValue;
    } else {
      // handle multiple partitioning case (year and month etc.), find the target partition dir
      try {
        partitionDir = findTargetPartitionFolder(modifTime, partitionFields, parquetFiles);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    // construct the path
    String fileName = "part-" + System.currentTimeMillis() + "-" + UUID.randomUUID() + ".parquet";
    Path outputFile = new Path(new Path(rootPath, partitionDir), fileName);
    // then inject/append/write it in the right partition
    MessageType schema = getParquetFileConfig(conf, new Path(rootPath)).getSchema();
    appendNewParquetFiles(outputFile, new Path(rootPath), fileToAppend, schema);

    // OR
    /*FileSystem fs = outputFile.getFileSystem(conf);
    if (!fs.exists(outputFile.getParent())) {
      fs.mkdirs(outputFile.getParent());
    }
    // copy a seperate file into the target partition folder
    FileUtil.copy(fs, fileToAppend, fs, outputFile, false, conf);*/
  }

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

  // append a file into a table
  public void appendNewParquetFiles(
      Path outputPath, Path filePath, Path fileToAppend, MessageType schema) throws IOException {
    Configuration conf = new Configuration();
    ParquetFileWriter writer =
        new ParquetFileWriter(
            HadoopOutputFile.fromPath(outputPath, conf),
            schema,
            ParquetFileWriter.Mode.CREATE,
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
    // save its modification time (for later sync related retrieval) in the metadata of the output
    // table
    combinedMeta.put(newKey, String.valueOf(fileStatus.getModificationTime()));
    combinedMeta.put("total_appends", String.valueOf(appendCount + 1));
    writer.end(combinedMeta);
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
