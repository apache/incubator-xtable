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
 * An index over a table that maps the values of a column to the rows that hold them, so an engine
 * can find those rows without a full table scan or join.
 *
 * @param <T> The table type of the source format (for example an Iceberg {@code Table})
 */
public interface Index<T> {
  /**
   * Checks whether the index of the given column exists.
   *
   * @param columnName The indexed column
   * @return true when a build of the index has completed
   */
  boolean doesIndexExist(String columnName);

  /**
   * Brings the indexes of the configured columns up to date with the current state of the table.
   * The indexed columns are part of the index configuration, not of the sync.
   *
   * @param table The table to index
   */
  void syncIndex(T table);

  /**
   * Looks up the given keys in the index of a column.
   *
   * @param table The table the index belongs to
   * @param keys The values to look up, in the column named {@code columnName}
   * @param columnName The indexed column
   * @return the matches of the keys. The schema of the result depends on the type of the index.
   */
  Dataset<Row> lookup(T table, Dataset<Row> keys, String columnName);
}
