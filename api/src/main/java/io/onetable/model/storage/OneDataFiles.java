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
 
package io.onetable.model.storage;

import java.util.List;
import java.util.Map;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Singular;
import lombok.Value;

import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.SchemaVersion;
import io.onetable.model.stat.ColumnStat;
import io.onetable.model.stat.Range;
import io.onetable.spi.OneTableSnapshotVisitor;

/**
 * Represents a grouping of data files, extends {@link OneDataFile} All properties of {@link
 * OneDataFile} also applies for {@link OneDataFiles} Setting a property at the {@link OneDataFiles}
 * level would override it for all the {@link OneDataFile} in the collection.
 *
 * @since 0.1
 */
@Value
@EqualsAndHashCode(callSuper = true)
public class OneDataFiles extends OneDataFile {
  @Singular List<OneDataFile> files;

  @Builder(builderMethodName = "collectionBuilder")
  public OneDataFiles(
      SchemaVersion schemaVersion,
      String physicalPath,
      FileFormat fileFormat,
      Map<OnePartitionField, Range> partitionValues,
      String partitionPath,
      Map<OneField, ColumnStat> columnStats,
      List<OneDataFile> files) {
    super(
        schemaVersion,
        physicalPath,
        fileFormat,
        partitionValues,
        partitionPath,
        0L,
        0L,
        columnStats,
        0L);
    this.files = files;
  }

  public void acceptVisitor(OneTableSnapshotVisitor defaultDataFileVisitor) {
    defaultDataFileVisitor.visit(this);
  }
}
