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

/**
 * Known types of storage files. For e.g. data file of a table, a position deletion, statistics
 * file.
 *
 * @since 0.1
 */
public enum FileType {
  /** Files of type data contain the actual data records of the table. */
  DATA_FILE,

  /**
   * Files of type deletion contain information of soft deleted records of the table, typically
   * containing ordinal references
   */
  DELETION_FILE,

  /**
   * Files of type statistics typically contain supplemental statistics information related to a
   * table, its partitions, or data files
   */
  STATISTICS_FILE,

  /** Files of type index contain information related to the indexes of a table */
  INDEX_FILE
}
