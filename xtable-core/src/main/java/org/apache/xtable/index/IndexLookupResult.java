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

import java.io.Serializable;

import lombok.Builder;
import lombok.Value;

import org.apache.spark.sql.catalyst.InternalRow;

/** Location of the row that holds a looked up key. */
@Value
@Builder
public class IndexLookupResult implements Serializable {
  private static final long serialVersionUID = 1L;

  /** The indexed column value that was looked up. */
  String key;

  /** Full path of the data file that holds the row. */
  String file;

  /** Zero based position of the row within the file. */
  long position;

  /**
   * Partition values of the row in the table's partition type, or null for unpartitioned tables.
   */
  InternalRow partition;
}
