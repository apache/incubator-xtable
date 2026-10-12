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
 * Builds and maintains an index over a table that maps the values of a column to the rows that hold
 * them. Reading the index is a separate concern, see {@link IndexReader}, so a writer does not
 * depend on the engine that queries the index.
 *
 * @param <T> The type that identifies the indexed table
 */
public interface IndexWriter<T> {
  /**
   * Builds the index on the first sync of the table, and on later syncs applies the commits that
   * the table has made since the previous sync. The index configuration, not this call, chooses the
   * indexed column.
   *
   * @param table The table to index
   */
  void syncIndex(T table);
}
