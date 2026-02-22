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
import java.net.URI;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.functional.RemoteIterators;

import org.apache.xtable.exception.ReadException;

/**
 * Manages Parquet File's Metadata
 *
 * <p>This class provides functions to handle Parquet metadata, creating metadata objects from
 * parquet files and filtering the files based on the modification times.
 */
@Log4j2
@RequiredArgsConstructor
public class ParquetDataManager {
  private final Configuration hadoopConf;
  private final String basePath;
  private final FileSystem fileSystem;

  public ParquetDataManager(Configuration hadoopConf, String basePath) {
    this.hadoopConf = hadoopConf;
    this.basePath = basePath;
    try {
      URI uri = Paths.get(basePath).toUri();
      this.fileSystem = FileSystem.get(uri, hadoopConf);
    } catch (IOException e) {
      throw new ReadException("Unable to initialize file system for base path: " + basePath, e);
    }
  }

  @Getter(value = AccessLevel.PRIVATE, lazy = true)
  private final List<LocatedFileStatus> parquetFiles = loadParquetFiles();

  ParquetFileInfo getMostRecentParquetFile() {
    LocatedFileStatus file =
        getParquetFiles().stream()
            .max(Comparator.comparing(LocatedFileStatus::getModificationTime))
            .orElseThrow(() -> new IllegalStateException("No files found"));
    return new ParquetFileInfo(hadoopConf, file);
  }

  ParquetFileInfo getParquetDataFileAt(long targetTime) {
    return getParquetFiles().stream()
        .filter(file -> file.getModificationTime() >= targetTime)
        .min(Comparator.comparing(LocatedFileStatus::getModificationTime))
        .map(file -> new ParquetFileInfo(hadoopConf, file))
        .orElseThrow(() -> new IllegalStateException("No file found at or after " + targetTime));
  }

  private List<LocatedFileStatus> loadParquetFiles() {
    try {
      RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path(basePath), true);
      return RemoteIterators.toList(iterator).stream()
          .filter(file -> file.getPath().getName().endsWith("parquet"))
          .collect(Collectors.toList());
    } catch (IOException e) {
      throw new ReadException("Unable to read files from file system", e);
    }
  }

  Stream<ParquetFileInfo> getCurrentFileInfo() {
    return getParquetFiles().stream()
        .map(fileStatus -> new ParquetFileInfo(hadoopConf, fileStatus));
  }

  List<ParquetFileInfo> getParquetFilesMetadataAfterTime(long syncTime) {
    return getParquetFiles().stream()
        .filter(file -> file.getModificationTime() >= syncTime)
        .map(file -> new ParquetFileInfo(hadoopConf, file))
        .collect(Collectors.toList());
  }
}
