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
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import lombok.Builder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;

import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.stat.PartitionValue;
import org.apache.xtable.model.stat.Range;
import org.apache.xtable.model.storage.InternalDataFile;

@Builder
public class ParquetDataManager {
  private ParquetMetadataExtractor metadataExtractor = ParquetMetadataExtractor.getInstance();

  public ParquetReader readParquetDataAsReader(String filePath, Configuration conf) {
    ParquetReader reader = null;
    Path file = new Path(filePath);
    try {
      reader = ParquetReader.builder(new GroupReadSupport(), file).withConf(conf).build();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return reader;
  }

  // partition fields are already computed, given a parquet file InternalDataFile must be derived
  // (e.g., using createInternalDataFileFromParquetFile())
  private Path appendNewParquetFile(
      Configuration conf,
      String rootPath,
      InternalDataFile internalParquetFile,
      List<InternalPartitionField> partitionFields) {
    Path finalFile = null;
    String partitionDir = "";
    List<PartitionValue> partitionValues = internalParquetFile.getPartitionValues();
    Instant modifTime = Instant.ofEpochMilli(internalParquetFile.getLastModified());
    Path fileToAppend = new Path(internalParquetFile.toString());
    // construct the file path to inject into the existing partitioned file
    if (partitionValues == null || partitionValues.isEmpty()) {
      String partitionValue =
          DateTimeFormatter.ISO_LOCAL_DATE.format(
              modifTime.atZone(ZoneId.systemDefault()).toLocalDate());
      partitionDir = partitionFields.get(0).getSourceField().getName() + partitionValue;
    } else {
      // handle multiple partitioning case (year and month etc.)
      partitionDir =
          partitionValues.stream()
              .map(
                  pv -> {
                    Range epochValueObject = pv.getRange();
                    // epochValueObject is always sure to be long
                    String valueStr = String.valueOf(epochValueObject.getMaxValue());
                    return pv.getPartitionField() + "=" + valueStr;
                  })
              .collect(Collectors.joining("/"));
    }
    String fileName = "part-" + System.currentTimeMillis() + "-" + UUID.randomUUID() + ".parquet";
    Path outputFile = new Path(new Path(rootPath, partitionDir), fileName);
    // return its reader for convenience of writing
    ParquetReader reader = readParquetDataAsReader(fileToAppend.getName(), conf);
    // append/write it in the right partition
    finalFile = writeNewParquetFile(conf, reader, fileToAppend, outputFile);
    return finalFile;
  }

  private ParquetFileConfig getParquetFileConfig(Configuration conf, Path fileToAppend) {
    ParquetFileConfig parquetFileConfig = new ParquetFileConfig(conf, fileToAppend);
    return parquetFileConfig;
  }

  private Path writeNewParquetFile(
      Configuration conf, ParquetReader reader, Path fileToAppend, Path outputFile) {
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

      e.printStackTrace();
    }
    return outputFile;
  }
}
