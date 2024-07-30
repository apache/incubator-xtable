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
 
package org.apache.xtable.delta;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import org.apache.spark.sql.delta.DeltaConfigs;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.DeltaOperations;
import org.apache.spark.sql.delta.OptimisticTransaction;
import org.apache.spark.sql.delta.actions.Action;
import org.apache.spark.sql.delta.actions.AddFile;
import org.apache.spark.sql.delta.actions.Format;
import org.apache.spark.sql.delta.actions.Metadata;
import org.apache.spark.sql.delta.actions.RemoveFile;

import scala.Option;
import scala.Some;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import com.google.common.annotations.VisibleForTesting;

import org.apache.xtable.conversion.PerTableConfig;
import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.metadata.TableSyncMetadata;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.storage.DataFilesDiff;
import org.apache.xtable.model.storage.PartitionFileGroup;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.spi.sync.ConversionTarget;

public class DeltaConversionTarget implements ConversionTarget {
  private static final String MIN_READER_VERSION = String.valueOf(1);
  // gets access to generated columns.
  private static final String MIN_WRITER_VERSION = String.valueOf(4);

  private DeltaLog deltaLog;
  private DeltaSchemaExtractor schemaExtractor;
  private DeltaPartitionExtractor partitionExtractor;
  private DeltaDataFileUpdatesExtractor dataFileUpdatesExtractor;

  private String tableName;
  private int logRetentionInHours;
  private TransactionState transactionState;

  public DeltaConversionTarget() {}

  public DeltaConversionTarget(PerTableConfig perTableConfig, SparkSession sparkSession) {
    this(
        perTableConfig.getTableDataPath(),
        perTableConfig.getTableName(),
        perTableConfig.getTargetMetadataRetentionInHours(),
        sparkSession,
        DeltaSchemaExtractor.getInstance(),
        DeltaPartitionExtractor.getInstance(),
        DeltaDataFileUpdatesExtractor.builder().build());
  }

  @VisibleForTesting
  DeltaConversionTarget(
      String tableDataPath,
      String tableName,
      int logRetentionInHours,
      SparkSession sparkSession,
      DeltaSchemaExtractor schemaExtractor,
      DeltaPartitionExtractor partitionExtractor,
      DeltaDataFileUpdatesExtractor dataFileUpdatesExtractor) {

    _init(
        tableDataPath,
        tableName,
        logRetentionInHours,
        sparkSession,
        schemaExtractor,
        partitionExtractor,
        dataFileUpdatesExtractor);
  }

  private void _init(
      String tableDataPath,
      String tableName,
      int logRetentionInHours,
      SparkSession sparkSession,
      DeltaSchemaExtractor schemaExtractor,
      DeltaPartitionExtractor partitionExtractor,
      DeltaDataFileUpdatesExtractor dataFileUpdatesExtractor) {
    DeltaLog deltaLog = DeltaLog.forTable(sparkSession, tableDataPath);
    boolean deltaTableExists = deltaLog.tableExists();
    if (!deltaTableExists) {
      deltaLog.ensureLogDirectoryExist();
    }
    this.schemaExtractor = schemaExtractor;
    this.partitionExtractor = partitionExtractor;
    this.dataFileUpdatesExtractor = dataFileUpdatesExtractor;
    this.deltaLog = deltaLog;
    this.tableName = tableName;
    this.logRetentionInHours = logRetentionInHours;
  }

  @Override
  public void init(PerTableConfig perTableConfig, Configuration configuration) {
    SparkSession sparkSession = DeltaConversionUtils.buildSparkSession(configuration);

    _init(
        perTableConfig.getTableDataPath(),
        perTableConfig.getTableName(),
        perTableConfig.getTargetMetadataRetentionInHours(),
        sparkSession,
        DeltaSchemaExtractor.getInstance(),
        DeltaPartitionExtractor.getInstance(),
        DeltaDataFileUpdatesExtractor.builder().build());
  }

  @Override
  public void beginSync(InternalTable table) {
    this.transactionState =
        new TransactionState(deltaLog, tableName, table.getLatestCommitTime(), logRetentionInHours);
  }

  @Override
  public void syncSchema(InternalSchema schema) {
    transactionState.setLatestSchema(schema);
  }

  @Override
  public void syncPartitionSpec(List<InternalPartitionField> partitionSpec) {
    Map<String, StructField> spec = partitionExtractor.convertToDeltaPartitionFormat(partitionSpec);
    if (partitionSpec != null) {
      for (Map.Entry<String, StructField> e : spec.entrySet()) {
        transactionState.getPartitionColumns().add(e.getKey());
        if (e.getValue() != null
            && transactionState.getLatestSchema().getFieldIndex(e.getValue().name()).isEmpty()) {
          // add generated columns to schema.
          transactionState.addColumn(e.getValue());
        }
      }
    }
  }

  @Override
  public void syncMetadata(TableSyncMetadata metadata) {
    transactionState.setMetadata(metadata);
  }

  @Override
  public void syncFilesForSnapshot(List<PartitionFileGroup> partitionedDataFiles) {
    transactionState.setActions(
        dataFileUpdatesExtractor.applySnapshot(
            deltaLog, partitionedDataFiles, transactionState.getLatestSchemaInternal()));
  }

  @Override
  public void syncFilesForDiff(DataFilesDiff dataFilesDiff) {
    transactionState.setActions(
        dataFileUpdatesExtractor.applyDiff(
            dataFilesDiff,
            transactionState.getLatestSchemaInternal(),
            deltaLog.dataPath().toString()));
  }

  @Override
  public void completeSync() {
    transactionState.commitTransaction();
    transactionState = null;
  }

  @Override
  public Optional<TableSyncMetadata> getTableMetadata() {
    return TableSyncMetadata.fromJson(
        deltaLog
            .snapshot()
            .metadata()
            .configuration()
            .getOrElse(TableSyncMetadata.XTABLE_METADATA, () -> null));
  }

  @Override
  public String getTableFormat() {
    return TableFormat.DELTA;
  }

  @EqualsAndHashCode
  @ToString
  private class TransactionState {
    private final OptimisticTransaction transaction;
    private final Instant commitTime;
    private final DeltaLog deltaLog;
    private final int retentionInHours;
    @Getter private final List<String> partitionColumns;
    private final String tableName;
    @Getter private StructType latestSchema;
    @Getter private InternalSchema latestSchemaInternal;
    @Setter private TableSyncMetadata metadata;
    @Setter private Seq<Action> actions;

    private TransactionState(
        DeltaLog deltaLog, String tableName, Instant latestCommitTime, int retentionInHours) {
      this.deltaLog = deltaLog;
      this.transaction = deltaLog.startTransaction();
      this.latestSchema = deltaLog.snapshot().schema();
      this.commitTime = latestCommitTime;
      this.partitionColumns = new ArrayList<>();
      this.tableName = tableName;
      this.retentionInHours = retentionInHours;
    }

    private void addColumn(StructField field) {
      latestSchema = latestSchema.add(field);
      latestSchemaInternal = schemaExtractor.toInternalSchema(latestSchema);
    }

    private void setLatestSchema(InternalSchema schema) {
      this.latestSchemaInternal = schema;
      this.latestSchema = schemaExtractor.fromInternalSchema(schema);
    }

    private void commitTransaction() {
      Metadata metadata =
          new Metadata(
              deltaLog.tableId(),
              tableName,
              "",
              getFileFormat(),
              latestSchema.json(),
              JavaConverters.asScalaBuffer(partitionColumns).toList(),
              ScalaUtils.convertJavaMapToScala(getConfigurationsForDeltaSync()),
              new Some<>(commitTime.toEpochMilli()));
      transaction.updateMetadata(metadata, false);
      transaction.commit(
          actions,
          new DeltaOperations.Update(Option.apply(Literal.fromObject("xtable-delta-sync"))));
    }

    private Map<String, String> getConfigurationsForDeltaSync() {
      Map<String, String> configMap = new HashMap<>();
      configMap.put(DeltaConfigs.MIN_READER_VERSION().key(), MIN_READER_VERSION);
      configMap.put(DeltaConfigs.MIN_WRITER_VERSION().key(), MIN_WRITER_VERSION);
      configMap.put(TableSyncMetadata.XTABLE_METADATA, metadata.toJson());
      // Sets retention for the Delta Log, does not impact underlying files in the table
      configMap.put(
          DeltaConfigs.LOG_RETENTION().key(), String.format("interval %d hours", retentionInHours));
      return configMap;
    }

    private Format getFileFormat() {
      if (actions.iterator().hasNext()) {
        // set file format based on action
        Action action = actions.iterator().next();
        String path = null;
        if (action instanceof AddFile) {
          path = ((AddFile) action).path();
        } else if (action instanceof RemoveFile) {
          path = ((RemoveFile) action).path();
        }
        if (path != null) {
          if (path.contains(".parquet")) {
            return new Format("parquet", ScalaUtils.convertJavaMapToScala(Collections.emptyMap()));
          } else if (path.contains(".orc")) {
            return new Format("orc", ScalaUtils.convertJavaMapToScala(Collections.emptyMap()));
          }
          throw new NotSupportedException("Fileformat is not supported for delta sync");
        }
      }
      // fallback to existing deltalog value
      return deltaLog.snapshot().metadata().format();
    }
  }
}
