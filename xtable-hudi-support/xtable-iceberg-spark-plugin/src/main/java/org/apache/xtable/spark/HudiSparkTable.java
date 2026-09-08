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

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.DeltaWriteBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.RowLevelOperation;
import org.apache.spark.sql.connector.write.RowLevelOperationBuilder;
import org.apache.spark.sql.connector.write.RowLevelOperationInfo;
import org.apache.spark.sql.connector.write.SupportsDelta;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

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

  /** A write plan the rule left alone reaches here; by default that is an error, not a bypass. */
  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    checkForeignWriteAllowed("write");
    return super.newWriteBuilder(info);
  }

  /**
   * Spark builds the row-level operation while it is still resolving the statement, before the
   * plugin's rules can claim it, so the check happens at the write builder, which only a plan that
   * really goes to Iceberg's writer ever asks for.
   */
  @Override
  public RowLevelOperationBuilder newRowLevelOperationBuilder(RowLevelOperationInfo info) {
    RowLevelOperationBuilder delegate = super.newRowLevelOperationBuilder(info);
    String what = info.command().toString();
    return () -> {
      RowLevelOperation operation = delegate.build();
      return operation instanceof SupportsDelta
          ? new GuardedDeltaOperation((SupportsDelta) operation, what)
          : new GuardedOperation(operation, what);
    };
  }

  private void checkForeignWriteAllowed(String what) {
    String mode =
        SparkSession.active()
            .conf()
            .get(HudiIcebergConf.FOREIGN_WRITES, HudiIcebergConf.FOREIGN_WRITES_REJECT);
    if (HudiIcebergConf.FOREIGN_WRITES_ALLOW.equalsIgnoreCase(mode)) {
      return;
    }
    throw new UnsupportedOperationException(
        String.format(
            "%s on Hudi-managed Iceberg table %s was not routed through Hudi (see the warning "
                + "logged by HudiIcebergWriteRule for the reason). Set %s=%s to let Iceberg write it "
                + "directly, or unset the table property %s to leave Hudi management.",
            what,
            name(),
            HudiIcebergConf.FOREIGN_WRITES,
            HudiIcebergConf.FOREIGN_WRITES_ALLOW,
            HudiIcebergConf.TABLE_PROP_MANAGED));
  }

  private class GuardedOperation implements RowLevelOperation {
    protected final RowLevelOperation delegate;
    protected final String what;

    GuardedOperation(RowLevelOperation delegate, String what) {
      this.delegate = delegate;
      this.what = what;
    }

    @Override
    public String description() {
      return delegate.description();
    }

    @Override
    public Command command() {
      return delegate.command();
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
      return delegate.newScanBuilder(options);
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
      checkForeignWriteAllowed(what);
      return delegate.newWriteBuilder(info);
    }

    @Override
    public NamedReference[] requiredMetadataAttributes() {
      return delegate.requiredMetadataAttributes();
    }
  }

  private class GuardedDeltaOperation extends GuardedOperation implements SupportsDelta {
    GuardedDeltaOperation(SupportsDelta delegate, String what) {
      super(delegate, what);
    }

    @Override
    public DeltaWriteBuilder newWriteBuilder(LogicalWriteInfo info) {
      checkForeignWriteAllowed(what);
      return ((SupportsDelta) delegate).newWriteBuilder(info);
    }

    @Override
    public NamedReference[] rowId() {
      return ((SupportsDelta) delegate).rowId();
    }
  }
}
