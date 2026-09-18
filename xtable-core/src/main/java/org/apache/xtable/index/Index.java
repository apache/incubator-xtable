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

import org.apache.spark.rdd.RDD;

/**
 * A secondary index over a table in another format, backed by XTable's Hudi conversion. It maps the
 * values of an indexed column to the data file and row position that holds each value, so an engine
 * can locate the rows to merge or delete without a full table join.
 *
 * @param <T> The table type of the source format (for example an Iceberg {@code Table})
 */
public interface Index<T> {

  /**
   * Checks whether a secondary index exists for the given column.
   *
   * @param columnName The indexed column
   * @return true when the index has been built at least once
   */
  boolean doesIndexExist(String columnName);

  /**
   * Builds or updates the secondary index for the given column from the current state of the table.
   *
   * @param table The table to index
   * @param columnName The column to index
   */
  void syncIndex(T table, String columnName);

  /**
   * Looks up the given keys in the secondary index of a column.
   *
   * @param table The table the index belongs to
   * @param keys The values of the indexed column to look up
   * @param columnName The indexed column
   * @return one {@link IndexLookupResult} for every key present in the index
   */
  RDD<IndexLookupResult> lookup(T table, RDD<String> keys, String columnName);
}
