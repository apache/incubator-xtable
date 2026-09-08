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

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.SupportsDynamicOverwrite;
import org.apache.spark.sql.connector.write.SupportsOverwriteV2;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.spark.Spark3Util;

/**
 * Wraps Iceberg's write builder for a managed table. A batch write that reaches it was not routed
 * by the rule and is refused (or allowed, per config). A streaming write keeps Iceberg's per-task
 * writers, which stage each micro-batch as parquet files, and commits every epoch through Hudi: the
 * staged files are read back, written by Hudi, and removed. Hudi's commit then publishes the
 * Iceberg snapshot as for any other write, so {@code writeStream.format("iceberg").toTable(...)}
 * needs no change.
 */
class HudiWriteBuilder implements WriteBuilder, SupportsDynamicOverwrite, SupportsOverwriteV2 {
  private static final Logger LOG = LoggerFactory.getLogger(HudiWriteBuilder.class);

  private final HudiSparkTable table;
  private final LogicalWriteInfo info;
  private final Runnable foreignWriteCheck;
  private WriteBuilder delegate;

  HudiWriteBuilder(
      HudiSparkTable table,
      LogicalWriteInfo info,
      WriteBuilder delegate,
      Runnable foreignWriteCheck) {
    this.table = table;
    this.info = info;
    this.delegate = delegate;
    this.foreignWriteCheck = foreignWriteCheck;
  }

  @Override
  public WriteBuilder overwriteDynamicPartitions() {
    delegate = ((SupportsDynamicOverwrite) delegate).overwriteDynamicPartitions();
    return this;
  }

  @Override
  public WriteBuilder overwrite(Predicate[] predicates) {
    delegate = ((SupportsOverwriteV2) delegate).overwrite(predicates);
    return this;
  }

  @Override
  public boolean canOverwrite(Predicate[] predicates) {
    return ((SupportsOverwriteV2) delegate).canOverwrite(predicates);
  }

  @Override
  public Write build() {
    Write write = delegate.build();
    return new Write() {
      @Override
      public String description() {
        return "HudiIcebergWrite(" + table.name() + ")";
      }

      @Override
      public BatchWrite toBatch() {
        foreignWriteCheck.run();
        return write.toBatch();
      }

      @Override
      public StreamingWrite toStreaming() {
        return new EpochThroughHudi(write.toStreaming());
      }

      @Override
      public CustomMetric[] supportedCustomMetrics() {
        return write.supportedCustomMetrics();
      }
    };
  }

  private class EpochThroughHudi implements StreamingWrite {
    private final StreamingWrite staging;

    EpochThroughHudi(StreamingWrite staging) {
      this.staging = staging;
    }

    @Override
    public StreamingDataWriterFactory createStreamingWriterFactory(PhysicalWriteInfo physicalInfo) {
      return staging.createStreamingWriterFactory(physicalInfo);
    }

    @Override
    public void commit(long epochId, WriterCommitMessage[] messages) {
      SparkSession spark = SparkSession.active();
      List<String> staged = stagedFiles(messages);
      LOG.info(
          "Committing epoch {} of query {} to {} through Hudi ({} staged files)",
          epochId,
          info.queryId(),
          table.name(),
          staged.size());
      try {
        if (!staged.isEmpty()) {
          Dataset<Row> rows =
              spark.read().schema(info.schema()).parquet(staged.toArray(new String[0]));
          List<Column> columns = new ArrayList<>();
          for (StructField field : table.schema().fields()) {
            columns.add(rows.col(field.name()));
          }
          new HudiIcebergRelation(spark, table, HudiWriteOperation.APPEND, catalogOf(spark), null)
              .insert(rows.select(columns.toArray(new Column[0])), false);
        }
      } finally {
        deleteStaged(spark, staged);
      }
    }

    @Override
    public void abort(long epochId, WriterCommitMessage[] messages) {
      staging.abort(epochId, messages);
    }
  }

  private TableCatalog catalogOf(SparkSession spark) {
    try {
      Spark3Util.CatalogAndIdentifier resolved =
          Spark3Util.catalogAndIdentifier(
              spark, java.util.Arrays.asList(table.name().split("\\.")));
      return resolved.catalog() instanceof TableCatalog ? (TableCatalog) resolved.catalog() : null;
    } catch (RuntimeException e) {
      return null;
    }
  }

  /**
   * Iceberg's commit messages carry the data files each task wrote; the class is package-private.
   */
  private static List<String> stagedFiles(WriterCommitMessage[] messages) {
    List<String> paths = new ArrayList<>();
    for (WriterCommitMessage message : messages) {
      if (message == null) {
        continue;
      }
      try {
        Method files = message.getClass().getDeclaredMethod("files");
        files.setAccessible(true);
        for (DataFile file : (DataFile[]) files.invoke(message)) {
          paths.add(file.path().toString());
        }
      } catch (ReflectiveOperationException e) {
        throw new IllegalStateException(
            "Cannot read staged files from " + message.getClass().getName(), e);
      }
    }
    return paths;
  }

  private static void deleteStaged(SparkSession spark, List<String> staged) {
    for (String path : staged) {
      try {
        Path p = new Path(path);
        FileSystem fs = p.getFileSystem(spark.sessionState().newHadoopConf());
        fs.delete(p, false);
      } catch (IOException e) {
        LOG.warn("Could not delete staged file {}", path, e);
      }
    }
  }
}
