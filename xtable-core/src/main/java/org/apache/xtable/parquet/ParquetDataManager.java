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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.Builder;
import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;

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

  public ParquetReader getParquetReader(String filePath, Configuration conf) throws IOException {
    ParquetReader reader = null;
    Path file = new Path(filePath);
    try {
      reader = ParquetReader.builder(new GroupReadSupport(), file).withConf(conf).build();
    } catch (IOException e) {
      log.error("Unexpected error during Parquet read: {}", filePath, e);
      throw new IOException("Unexpected error reading Parquet file", e);
    }
    return reader;
  }

  // TODO check if footer of added file can cause problems (catalog) when reading the full table
  // after appending
  // check required before appending the file
  private boolean checkSchemaIsSame(Configuration conf, Path fileToAppend, Path fileFromTable) {
    ParquetFileConfig schemaFileAppend = getParquetFileConfig(conf, fileToAppend);
    ParquetFileConfig schemaFileFromTable = getParquetFileConfig(conf, fileFromTable);
    return schemaFileAppend.getSchema().equals(schemaFileFromTable.getSchema());
  }

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
  // partition fields are already computed, given a parquet file InternalDataFile must be derived
  // (e.g., using createInternalDataFileFromParquetFile())
  private Path appendNewParquetFile(
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
    // return its reader for convenience of writing
    ParquetReader reader = getParquetReader(fileToAppend.getName(), conf);
    // then inject/append/write it in the right partition
    finalFile = writeNewParquetFile(conf, reader, fileToAppend, outputFile);
    return finalFile;
  }

  private ParquetFileConfig getParquetFileConfig(Configuration conf, Path fileToAppend) {
    ParquetFileConfig parquetFileConfig = new ParquetFileConfig(conf, fileToAppend);
    return parquetFileConfig;
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
}
