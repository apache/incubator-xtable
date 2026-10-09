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
 
package org.apache.xtable.index;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * A secondary index over a table in another format, backed by XTable's Hudi conversion. It maps the
 * values of an indexed column to the data file and row position that holds each value, so an engine
 * can locate the rows to merge or delete without a full table join.
 *
 * @param <T> The table type of the source format (for example an Iceberg {@code Table})
 */
public interface Index<T> {
  /** Result column with the full path of the data file that holds the row. */
  String FILE_COLUMN = "_file";

  /** Result column with the zero based position of the row within the file. */
  String POSITION_COLUMN = "_pos";

  /**
   * Checks whether a secondary index exists for the given column.
   *
   * @param columnName The indexed column
   * @return true when a build of the index has completed
   */
  boolean doesIndexExist(String columnName);

  /**
   * Brings the secondary indexes of the configured columns up to date with the current state of the
   * table. The indexed columns are part of the index configuration, not of the sync.
   *
   * @param table The table to index
   */
  void syncIndex(T table);

  /**
   * Looks up the given keys in the secondary index of a column.
   *
   * @param table The table the index belongs to
   * @param keys The values to look up, in the column named {@code columnName}
   * @param columnName The indexed column
   * @return one row for every row of the table that holds a key, with the key in the column named
   *     {@code columnName} and the location of the row in {@link #FILE_COLUMN} and {@link
   *     #POSITION_COLUMN}. A caller that needs other metadata of the row, such as its partition,
   *     reads the returned files through the table format.
   */
  Dataset<Row> lookup(T table, Dataset<Row> keys, String columnName);
}
