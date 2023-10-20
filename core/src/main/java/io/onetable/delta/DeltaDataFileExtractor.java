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
 
package io.onetable.delta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import lombok.Builder;

import org.apache.hadoop.fs.Path;

import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.actions.AddFile;

import io.onetable.exception.NotSupportedException;
import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.storage.FileFormat;
import io.onetable.model.storage.OneDataFile;
import io.onetable.model.storage.OneDataFiles;
import io.onetable.spi.extractor.PartitionedDataFileIterator;

/** DeltaDataFileExtractor lets the consumer iterate over partitions. */
@Builder
public class DeltaDataFileExtractor {

  @Builder.Default
  private final DeltaStatsExtractor fileStatsExtractor = DeltaStatsExtractor.getInstance();

  @Builder.Default
  private final DeltaPartitionExtractor partitionExtractor = DeltaPartitionExtractor.getInstance();

  /**
   * Initializes an iterator for Delta Lake files. This should only be used when column stats are
   * not required.
   *
   * @return Delta table file iterator, files returned do not have column stats set to reduce memory
   *     overhead
   */
  public PartitionedDataFileIterator iteratorWithoutStats(
      Snapshot deltaSnapshot, OneSchema schema) {
    return new DeltaDataFileIterator(deltaSnapshot, schema, false);
  }

  /**
   * Initializes an iterator for Delta Lake files.
   *
   * @return Delta table file iterator
   */
  public PartitionedDataFileIterator iterator(Snapshot deltaSnapshot, OneSchema schema) {
    return new DeltaDataFileIterator(deltaSnapshot, schema, true);
  }

  public class DeltaDataFileIterator implements PartitionedDataFileIterator {
    private final FileFormat fileFormat;
    private final List<OneField> fields;
    private final List<OnePartitionField> partitionFields;
    private final Iterator<OneDataFile> dataFilesIterator;
    private final String tableBasePath;
    private final boolean includeColumnStats;

    private DeltaDataFileIterator(Snapshot snapshot, OneSchema schema, boolean includeColumnStats) {
      this.fileFormat = convertToOneTableFileFormat(snapshot.metadata().format().provider());
      this.fields = schema.getFields();
      this.partitionFields =
          partitionExtractor.convertFromDeltaPartitionFormat(
              schema, snapshot.metadata().partitionSchema());
      this.tableBasePath = snapshot.deltaLog().dataPath().toUri().toString();
      this.includeColumnStats = includeColumnStats;
      this.dataFilesIterator =
          snapshot.allFiles().collectAsList().stream()
              .map(this::convertAddFileToOneDataFile)
              .collect(Collectors.toList())
              .listIterator();
    }

    @Override
    public void close() throws Exception {}

    @Override
    public boolean hasNext() {
      return this.dataFilesIterator.hasNext();
    }

    @Override
    public OneDataFiles next() {
      List<OneDataFile> dataFiles = new ArrayList<>();
      while (hasNext()) {
        dataFiles.add(this.dataFilesIterator.next());
      }
      return OneDataFiles.collectionBuilder().files(dataFiles).build();
    }

    private FileFormat convertToOneTableFileFormat(String provider) {
      if (provider.equals("parquet")) {
        return FileFormat.APACHE_PARQUET;
      } else if (provider.equals("orc")) {
        return FileFormat.APACHE_ORC;
      }
      throw new NotSupportedException(
          String.format("delta file format %s is not recognized", provider));
    }

    private OneDataFile convertAddFileToOneDataFile(AddFile addFile) {
      return OneDataFile.builder()
          .physicalPath(getFullPathToFile(addFile.path()))
          .fileFormat(fileFormat)
          .fileSizeBytes(addFile.getFileSize())
          .recordCount(addFile.getNumLogicalRecords())
          .lastModified(addFile.modificationTime())
          .partitionValues(
              partitionExtractor.partitionValueExtraction(
                  addFile.partitionValues(), partitionFields))
          .columnStats(
              includeColumnStats
                  ? fileStatsExtractor.getColumnStatsForFile(addFile, fields)
                  : Collections.emptyMap())
          .build();
    }

    private String getFullPathToFile(String path) {
      if (path.startsWith(tableBasePath)) {
        return path;
      }
      return tableBasePath + Path.SEPARATOR + path;
    }
  }
}
