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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.iceberg.catalog.Procedure;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.iceberg.spark.Spark3Util;

import scala.collection.JavaConverters;

/**
 * An Iceberg maintenance procedure that, on a Hudi-managed table, runs the equivalent Hudi table
 * service instead. Same name, same parameters, same output columns, so scheduled maintenance keeps
 * working unchanged. Non-managed tables go to the Iceberg procedure as before.
 */
class HudiRedirectedProcedure implements Procedure {
  private static final Logger LOG = LoggerFactory.getLogger(HudiRedirectedProcedure.class);

  static final String REWRITE_DATA_FILES = "rewrite_data_files";
  static final String EXPIRE_SNAPSHOTS = "expire_snapshots";
  static final String REWRITE_POSITION_DELETE_FILES = "rewrite_position_delete_files";

  private final String name;
  private final Procedure delegate;
  private final HudiSparkCatalog catalog;

  HudiRedirectedProcedure(String name, Procedure delegate, HudiSparkCatalog catalog) {
    this.name = name;
    this.delegate = delegate;
    this.catalog = catalog;
  }

  @Override
  public ProcedureParameter[] parameters() {
    return delegate.parameters();
  }

  @Override
  public StructType outputType() {
    return delegate.outputType();
  }

  @Override
  public String description() {
    return delegate.description() + " (runs the Hudi equivalent on Hudi-managed tables)";
  }

  @Override
  public InternalRow[] call(InternalRow args) {
    Identifier ident = tableIdentifier(args);
    Table table;
    try {
      table = catalog.loadTable(ident);
    } catch (Exception e) {
      return delegate.call(args);
    }
    if (!(table instanceof HudiSparkTable)) {
      return delegate.call(args);
    }
    HudiTableContext context = ((HudiSparkTable) table).context();
    Map<String, Object> result = new HashMap<>();
    try {
      if (REWRITE_DATA_FILES.equals(name)) {
        HudiTableServices.RewriteResult rewrite =
            HudiTableServices.cluster(SparkSession.active(), context, options(args));
        result.put("rewritten_data_files_count", rewrite.rewrittenFiles);
        result.put("added_data_files_count", rewrite.addedFiles);
        result.put("rewritten_bytes_count", rewrite.rewrittenBytes);
        catalog.invalidateTable(ident);
      } else if (REWRITE_POSITION_DELETE_FILES.equals(name)) {
        HudiTableServices.RewriteResult compaction =
            HudiTableServices.compact(SparkSession.active(), context);
        result.put("rewritten_delete_files_count", compaction.rewrittenFiles);
        result.put("added_delete_files_count", 0);
        result.put("rewritten_bytes_count", compaction.rewrittenBytes);
        result.put("added_bytes_count", compaction.rewrittenBytes);
        catalog.invalidateTable(ident);
      } else if (EXPIRE_SNAPSHOTS.equals(name)) {
        LOG.info(
            "expire_snapshots on Hudi-managed table {} is a no-op: Hudi's cleaner and archiver "
                + "expire the corresponding Iceberg snapshots after every write "
                + "(tune with the hudi.write.clean.* table properties)",
            ident);
      }
    } catch (Exception e) {
      throw new RuntimeException("Hudi table service failed for " + ident, e);
    }
    return new InternalRow[] {row(result)};
  }

  private Identifier tableIdentifier(InternalRow args) {
    ProcedureParameter[] params = delegate.parameters();
    int index = 0;
    for (int i = 0; i < params.length; i++) {
      if ("table".equals(params[i].name())) {
        index = i;
        break;
      }
    }
    String tableName = args.getString(index);
    List<String> parts;
    try {
      parts =
          JavaConverters.seqAsJavaListConverter(
                  SparkSession.active()
                      .sessionState()
                      .sqlParser()
                      .parseMultipartIdentifier(tableName))
              .asJava();
    } catch (org.apache.spark.sql.catalyst.parser.ParseException e) {
      throw new IllegalArgumentException("Cannot parse table identifier " + tableName, e);
    }
    return Spark3Util.catalogAndIdentifier(SparkSession.active(), parts, catalog).identifier();
  }

  private Map<String, String> options(InternalRow args) {
    ProcedureParameter[] params = delegate.parameters();
    Map<String, String> options = new HashMap<>();
    for (int i = 0; i < params.length; i++) {
      if ("options".equals(params[i].name()) && !args.isNullAt(i)) {
        org.apache.spark.sql.catalyst.util.MapData map = args.getMap(i);
        for (int j = 0; j < map.numElements(); j++) {
          options.put(
              map.keyArray().getUTF8String(j).toString(),
              map.valueArray().getUTF8String(j).toString());
        }
      }
    }
    return options;
  }

  private InternalRow row(Map<String, Object> values) {
    StructField[] fields = outputType().fields();
    Object[] cells = new Object[fields.length];
    for (int i = 0; i < fields.length; i++) {
      Object value = values.get(fields[i].name());
      if (value == null) {
        if (fields[i].dataType().equals(DataTypes.LongType)) {
          value = 0L;
        } else if (fields[i].dataType().equals(DataTypes.IntegerType)) {
          value = 0;
        }
      }
      cells[i] = value;
    }
    return new GenericInternalRow(cells);
  }
}
