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
 
package org.apache.xtable.model.storage;

import java.util.Collections;
import java.util.List;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.experimental.FieldDefaults;
import lombok.experimental.SuperBuilder;

import org.apache.xtable.model.stat.ColumnStat;
import org.apache.xtable.model.stat.PartitionValue;
import java.util.Objects;
/**
 * Represents a data file in the table.
 *
 * @since 0.1
 */
@SuperBuilder(toBuilder = true)
@FieldDefaults(makeFinal = true, level = lombok.AccessLevel.PRIVATE)
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@Getter
public class InternalDataFile extends InternalFile {
  // file format
  @Builder.Default @NonNull FileFormat fileFormat = FileFormat.APACHE_PARQUET;
  // partition ranges for the data file
  @Builder.Default @NonNull List<PartitionValue> partitionValues = Collections.emptyList();

  // column stats for each column in the data file
  @Builder.Default @NonNull List<ColumnStat> columnStats = Collections.emptyList();
  // last modified time in millis since epoch
  long lastModified;
  public static InternalDataFileBuilder builderFrom(InternalDataFile dataFile) {
    return dataFile.toBuilder();
  }

  public static boolean compareFiles(InternalDataFile obj1, InternalDataFile obj2) {
    if (obj2 == obj1) {
      return true;
    }
    if (obj2 != null && obj2 instanceof InternalDataFile) {
      InternalDataFile other = (InternalDataFile) obj2;
      return Objects.equals(obj1.getPhysicalPath(), other.getPhysicalPath()) &&
              Objects.equals(obj1.getLastModified(), other.getLastModified()) &&
              other.getColumnStats().equals(obj2.getColumnStats());// add partitionFilds equality
    }
    return false;
  }
}
