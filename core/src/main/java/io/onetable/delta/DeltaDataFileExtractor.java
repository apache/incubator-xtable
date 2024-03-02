/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.onetable.delta;

import java.util.Iterator;
import java.util.List;

import lombok.Builder;

import org.apache.spark.sql.delta.Snapshot;

import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.storage.FileFormat;
import io.onetable.model.storage.OneDataFile;
import io.onetable.spi.extractor.DataFileIterator;

/** DeltaDataFileExtractor lets the consumer iterate over partitions. */
@Builder
public class DeltaDataFileExtractor {

  @Builder.Default
  private final DeltaPartitionExtractor partitionExtractor = DeltaPartitionExtractor.getInstance();

  @Builder.Default
  private final DeltaStatsExtractor fileStatsExtractor = DeltaStatsExtractor.getInstance();

  @Builder.Default
  private final DeltaActionsConverter actionsConverter = DeltaActionsConverter.getInstance();

  /**
   * Initializes an iterator for Delta Lake files.
   *
   * @return Delta table file iterator
   */
  public DataFileIterator iterator(Snapshot deltaSnapshot, OneSchema schema) {
    return new DeltaDataFileIterator(deltaSnapshot, schema, true);
  }

  public class DeltaDataFileIterator implements DataFileIterator {
    private final FileFormat fileFormat;
    private final List<OneField> fields;
    private final List<OnePartitionField> partitionFields;
    private final Iterator<OneDataFile> dataFilesIterator;

    private DeltaDataFileIterator(Snapshot snapshot, OneSchema schema, boolean includeColumnStats) {
      this.fileFormat =
          actionsConverter.convertToOneTableFileFormat(snapshot.metadata().format().provider());
      this.fields = schema.getFields();
      this.partitionFields =
          partitionExtractor.convertFromDeltaPartitionFormat(
              schema, snapshot.metadata().partitionSchema());
      this.dataFilesIterator =
          snapshot.allFiles().collectAsList().stream()
              .map(
                  addFile ->
                      actionsConverter.convertAddActionToOneDataFile(
                          addFile,
                          snapshot,
                          fileFormat,
                          partitionFields,
                          fields,
                          includeColumnStats,
                          partitionExtractor,
                          fileStatsExtractor))
              .iterator();
    }

    @Override
    public void close() throws Exception {}

    @Override
    public boolean hasNext() {
      return this.dataFilesIterator.hasNext();
    }

    @Override
    public OneDataFile next() {
      return dataFilesIterator.next();
    }
  }
}
