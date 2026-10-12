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

/**
 * Reads an index that an {@link IndexWriter} maintains, so an engine can find the rows that hold
 * given values of a column without a full table scan or join. The key and result types depend on
 * the engine that runs the lookup, for example a Spark {@code Dataset<Row>}.
 *
 * @param <T> The type that identifies the indexed table
 * @param <K> The type of the keys to look up
 * @param <R> The type of the lookup result
 */
public interface IndexReader<T, K, R> {
  /**
   * Checks whether the index of the given column exists.
   *
   * @param columnName The indexed column
   * @return true when a build of the index has completed
   */
  boolean doesIndexExist(String columnName);

  /**
   * Looks up the given keys in the index of a column.
   *
   * @param table The table the index belongs to
   * @param keys The values to look up, in the column named {@code columnName}
   * @param columnName The indexed column
   * @return the matches of the keys. The schema of the result depends on the type of the index.
   */
  R lookup(T table, K keys, String columnName);
}
