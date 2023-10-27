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

import java.util.Collections;
import java.util.Map;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.SchemaVersion;
import io.onetable.model.stat.ColumnStat;
import io.onetable.model.stat.Range;

/**
 * Represents a data file in the table.
 *
 * @since 0.1
 */
@Getter
@Builder(toBuilder = true)
@EqualsAndHashCode
@ToString
public class OneDataFile {
  // written schema version
  protected final SchemaVersion schemaVersion;
  // physical path of the file
  protected final String physicalPath;
  // file format
  protected final FileFormat fileFormat;
  // partition ranges for the data file
  @Builder.Default
  protected final Map<OnePartitionField, Range> partitionValues =
      Collections.emptyMap(); // Partition path

  protected final String partitionPath;
  protected final long fileSizeBytes;
  protected final long recordCount;
  // column stats for each column in the data file
  @Builder.Default protected final Map<OneField, ColumnStat> columnStats = Collections.emptyMap();
  // last modified time in millis since epoch
  protected final long lastModified;
}
