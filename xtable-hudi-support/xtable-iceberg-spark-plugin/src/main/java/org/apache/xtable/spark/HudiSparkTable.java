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

import org.apache.spark.sql.connector.expressions.filter.Predicate;

import org.apache.iceberg.Table;
import org.apache.iceberg.spark.source.SparkTable;

/**
 * The Spark view of a Hudi-managed Iceberg table. Reads are plain Iceberg reads; the write plans
 * that target this table are rewritten by {@link HudiIcebergWriteRule} to go through Hudi.
 */
public class HudiSparkTable extends SparkTable {
  private final HudiTableContext context;
  private final boolean refreshEagerly;

  public HudiSparkTable(Table icebergTable, boolean refreshEagerly, HudiTableContext context) {
    super(icebergTable, refreshEagerly);
    this.context = context;
    this.refreshEagerly = refreshEagerly;
  }

  public HudiSparkTable(
      Table icebergTable, Long snapshotId, boolean refreshEagerly, HudiTableContext context) {
    super(icebergTable, snapshotId, refreshEagerly);
    this.context = context;
    this.refreshEagerly = refreshEagerly;
  }

  public HudiSparkTable(
      Table icebergTable, String branch, boolean refreshEagerly, HudiTableContext context) {
    super(icebergTable, branch, refreshEagerly);
    this.context = context;
    this.refreshEagerly = refreshEagerly;
  }

  public HudiTableContext context() {
    return context;
  }

  @Override
  public SparkTable copyWithSnapshotId(long snapshotId) {
    return new HudiSparkTable(table(), snapshotId, refreshEagerly, context);
  }

  @Override
  public SparkTable copyWithBranch(String targetBranch) {
    return new HudiSparkTable(table(), targetBranch, refreshEagerly, context);
  }

  /**
   * Iceberg answers true for partition predicates and then commits a metadata-only delete straight
   * from the table, which would bypass Hudi. Force every DELETE through the row-level path so the
   * write rule sees it.
   */
  @Override
  public boolean canDeleteWhere(Predicate[] predicates) {
    return false;
  }
}
