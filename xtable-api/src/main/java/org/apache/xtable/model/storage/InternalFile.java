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

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.experimental.FieldDefaults;
import lombok.experimental.SuperBuilder;

/**
 * This class is an internal representation of a logical storage file of a table and is the base
 * class of all types of storage files. The most common type of storage file is a data-file, which
 * contains the actual records of a table. Other examples of a storage file are positional delete
 * files (containing ordinals of the records to be deleted from a data file), stat files, and index
 * files. For completeness of the conversion process, XTable needs to recognize current storage
 * files of a table, and also the storage files that are added and removed over time as the state of
 * the table changes. Different table formats support different storage file types. This base file
 * representation is generic and extensible and is needed to design stable interfaces.
 */
@SuperBuilder(toBuilder = true)
@FieldDefaults(makeFinal = true, level = lombok.AccessLevel.PRIVATE)
@ToString(callSuper = true)
@EqualsAndHashCode
@AllArgsConstructor
@Getter
public abstract class InternalFile {
  // Absolute path of the storage file, with the scheme, that contains this logical file. Typically,
  // one physical storage file contains only one base file, for e.g. a parquet data file. However,
  // in some cases, one storage file can contain multiple logical storage files for optimizations.
  @NonNull String physicalPath;

  // The size of the logical file in the physical storage file.
  long fileSizeBytes;

  // The number of records in the storage file.
  long recordCount;
}
