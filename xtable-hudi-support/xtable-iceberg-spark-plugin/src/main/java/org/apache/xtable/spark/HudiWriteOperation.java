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
 
package org.apache.xtable.spark;

/** How a Spark write plan maps onto a Hudi write operation. */
public enum HudiWriteOperation {
  /** {@code AppendData}: upsert on a keyed table, insert on a key-less table. */
  APPEND(null),
  /** {@code OverwriteByExpression(true)}: replace every partition of the table. */
  INSERT_OVERWRITE_TABLE("insert_overwrite_table"),
  /**
   * {@code OverwritePartitionsDynamic} / static partition overwrite: replace touched partitions.
   */
  INSERT_OVERWRITE("insert_overwrite"),
  /** {@code UpdateTable} / {@code MergeIntoTable} on a keyed table: upsert the affected rows. */
  UPSERT("upsert"),
  /** {@code DeleteFromTable} on a keyed table: delete the affected rows by record key. */
  DELETE("delete");

  private final String hoodieOperation;

  HudiWriteOperation(String hoodieOperation) {
    this.hoodieOperation = hoodieOperation;
  }

  /** The value of {@code hoodie.datasource.write.operation}, or null to let Hudi decide. */
  public String hoodieOperation() {
    return hoodieOperation;
  }
}
