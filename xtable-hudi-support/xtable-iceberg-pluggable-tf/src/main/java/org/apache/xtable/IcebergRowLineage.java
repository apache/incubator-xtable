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
 
package org.apache.xtable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import org.apache.hudi.common.BaseFileColumnResolver;
import org.apache.hudi.common.FormatMetaColumn;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.util.HoodieStorageUtils;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;

import org.apache.xtable.exception.ReadException;
import org.apache.xtable.hudi.PathBasedPartitionSpecExtractor;
import org.apache.xtable.hudi.PathBasedPartitionValuesExtractor;
import org.apache.xtable.iceberg.IcebergPartitionValueConverter;
import org.apache.xtable.iceberg.IcebergSchemaExtractor;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.stat.PartitionValue;

/**
 * Row lineage of an Iceberg table at format version 3, kept across Hudi rewrites of base files.
 *
 * <p>Iceberg derives the {@code _row_id} of a row that a data file does not store from the file's
 * first row id plus the row's position, and its {@code _last_updated_sequence_number} from the
 * file's data sequence number. A rewrite moves rows to a new data file, which gets a new first row
 * id and sequence number, so the rows it carries over unchanged must store the values they had.
 * Both columns are declared to Hudi as format meta columns: Hudi stores them in every base file it
 * writes and resolves the values of a carried-over row from the base file it is read from. A row
 * that a write updates keeps the row id of the row it replaces and stores a null sequence number,
 * so it gets the write's sequence number, as in Iceberg, also when Hudi writes the update as a
 * delete and an insert into another file, as merge-on-read with deletion vectors does. A new row
 * stores null for both, so Iceberg gives it a new row id.
 */
final class IcebergRowLineage {
  private static final List<Types.NestedField> COLUMNS =
      Arrays.asList(MetadataColumns.ROW_ID, MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER);

  private static final int MAX_CACHED_TABLES = 16;

  /** Iceberg tables by Hudi base path, refreshed on every lookup. */
  private static final Map<String, Table> TABLES = lruCache(MAX_CACHED_TABLES);

  private static final int MAX_CACHED_PARTITIONS = 64;

  /**
   * Data files of the partitions of the snapshots last looked up, by table, snapshot, partition.
   */
  private static final Map<String, Map<String, BaseFileLineage>> LINEAGE_BY_PARTITION =
      lruCache(MAX_CACHED_PARTITIONS);

  /** Keeps the stored value, for a table whose Iceberg table has no row lineage. */
  private static final BaseFileColumnResolver STORED_VALUE =
      (rowPosition, storedValue) -> storedValue;

  private static final String WHOLE_TABLE = "";

  private IcebergRowLineage() {}

  static List<FormatMetaColumn> columns() {
    return Arrays.asList(new RowLineageColumn(true), new RowLineageColumn(false));
  }

  /** The first row id and data sequence number Iceberg has for one data file. */
  private static final class BaseFileLineage {
    private final Long firstRowId;
    private final long dataSequenceNumber;

    private BaseFileLineage(Long firstRowId, long dataSequenceNumber) {
      this.firstRowId = firstRowId;
      this.dataSequenceNumber = dataSequenceNumber;
    }
  }

  private static final class RowLineageColumn implements FormatMetaColumn {
    private final boolean rowId;

    private RowLineageColumn(boolean rowId) {
      this.rowId = rowId;
    }

    @Override
    public HoodieSchemaField field() {
      return HoodieSchemaField.of(
          column().name(),
          HoodieSchema.createNullable(HoodieSchemaType.LONG),
          null,
          HoodieSchema.NULL_VALUE);
    }

    private Types.NestedField column() {
      return rowId ? MetadataColumns.ROW_ID : MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER;
    }

    @Override
    public Object valueOnWrite(String instantTime) {
      return null;
    }

    @Override
    public Object valueOnUpdate(String instantTime, Object replacedValue) {
      return rowId ? replacedValue : null;
    }

    @Override
    public BaseFileColumnResolver resolverFor(
        HoodieTableMetaClient metaClient, HoodieBaseFile baseFile) {
      BaseFileLineage lineage = lookUp(metaClient, baseFile);
      if (lineage == null) {
        return STORED_VALUE;
      }
      return rowId
          ? new RowIdResolver(lineage.firstRowId)
          : new SequenceNumberResolver(lineage.dataSequenceNumber);
    }
  }

  private static final class RowIdResolver implements BaseFileColumnResolver {
    private final long firstRowId;

    private RowIdResolver(long firstRowId) {
      this.firstRowId = firstRowId;
    }

    @Override
    public Object valueAt(long rowPosition, Object storedValue) {
      return storedValue != null ? storedValue : (Object) (firstRowId + rowPosition);
    }
  }

  private static final class SequenceNumberResolver implements BaseFileColumnResolver {
    private final long dataSequenceNumber;

    private SequenceNumberResolver(long dataSequenceNumber) {
      this.dataSequenceNumber = dataSequenceNumber;
    }

    @Override
    public Object valueAt(long rowPosition, Object storedValue) {
      return storedValue != null ? storedValue : (Object) dataSequenceNumber;
    }
  }

  /**
   * Looks the base file up in the current Iceberg snapshot. A data file keeps its first row id only
   * while it stays in the table: a rollback that restores it adds it again with a new one, so only
   * the current snapshot gives the values Iceberg readers see.
   *
   * @return null when there is no row lineage to keep: the Iceberg table is below format version 3,
   *     or no row of the base file is live, as it holds none or a delete removed all of them, which
   *     the table format leaves out of Iceberg
   * @throws HoodieException when a base file with live rows is not in the current snapshot, or
   *     Iceberg has no first row id for it
   */
  private static BaseFileLineage lookUp(HoodieTableMetaClient metaClient, HoodieBaseFile baseFile) {
    String tableName = metaClient.getTableConfig().getTableName();
    Table table = cachedTable(metaClient);
    TableMetadata metadata = refresh(metaClient, table);
    if (metadata.formatVersion() < 3) {
      return null;
    }
    String path = IcebergTableFormat.qualifiedPath(metaClient, baseFile);
    Snapshot snapshot = metadata.currentSnapshot();
    if (snapshot == null) {
      throw notInSnapshot(tableName, path, null);
    }
    String partitionPath =
        FSUtils.getRelativePartitionPath(
            metaClient.getBasePath(), new StoragePath(baseFile.getPath()).getParent());
    BaseFileLineage lineage =
        lineageInPartition(
                table,
                metadata,
                snapshot,
                partitionPath,
                partitionFilter(metadata, metaClient, partitionPath))
            .get(path);
    if (lineage == null && metadata.spec().isPartitioned()) {
      // The partition filter only saves reading manifests and never decides the result.
      lineage =
          lineageInPartition(table, metadata, snapshot, WHOLE_TABLE, Expressions.alwaysTrue())
              .get(path);
    }
    if (lineage == null) {
      if (holdsNoLiveRows(metaClient, path)) {
        return null;
      }
      throw notInSnapshot(tableName, path, snapshot.snapshotId());
    }
    if (lineage.firstRowId == null) {
      throw new HoodieException(
          String.format(
              "Base file %s of table %s has no first row id in Iceberg snapshot %d, so the rows a"
                  + " rewrite carries over from it cannot keep their row lineage",
              path, tableName, snapshot.snapshotId()));
    }
    return lineage;
  }

  private static boolean holdsNoLiveRows(HoodieTableMetaClient metaClient, String path) {
    FileSystemViewManager viewManager =
        FileSystemViewManager.createViewManagerWithTableMetadata(
            new HoodieLocalEngineContext(metaClient.getStorageConf()),
            HoodieMetadataConfig.newBuilder()
                .enable(metaClient.getTableConfig().isMetadataTableAvailable())
                .build(),
            FileSystemViewStorageConfig.newBuilder().build(),
            HoodieCommonConfig.newBuilder().build());
    try {
      return new HudiPositionalDeleteExtractor(metaClient, viewManager).holdsNoLiveRows(path);
    } finally {
      viewManager.close();
    }
  }

  /** The Iceberg table of the Hudi table, loaded once per process and refreshed on every use. */
  private static Table cachedTable(HoodieTableMetaClient metaClient) {
    return TABLES.computeIfAbsent(
        metaClient.getBasePath().toString(),
        basePath -> IcebergTableFormat.icebergTable(metaClient));
  }

  /**
   * Refreshes the table, which reads the table metadata again only when it changed, and returns its
   * current metadata. A table that fails to refresh, as one dropped and created again does, is
   * loaded again.
   */
  private static TableMetadata refresh(HoodieTableMetaClient metaClient, Table table) {
    synchronized (table) {
      try {
        table.refresh();
        return ((HasTableOperations) table).operations().current();
      } catch (RuntimeException e) {
        TABLES.remove(metaClient.getBasePath().toString(), table);
      }
    }
    Table reloaded = cachedTable(metaClient);
    synchronized (reloaded) {
      return ((HasTableOperations) reloaded).operations().current();
    }
  }

  private static <V> Map<String, V> lruCache(int maxEntries) {
    return Collections.synchronizedMap(
        new LinkedHashMap<String, V>(16, 0.75f, true) {
          @Override
          protected boolean removeEldestEntry(Map.Entry<String, V> eldest) {
            return size() > maxEntries;
          }
        });
  }

  private static HoodieException notInSnapshot(String tableName, String path, Long snapshotId) {
    return new HoodieException(
        String.format(
            "Base file %s of table %s is not in the current Iceberg snapshot %s, so the rows a"
                + " rewrite carries over from it cannot keep their row lineage (%s, %s). Every"
                + " completed Hudi instant has to be recorded in Iceberg before a base file it"
                + " wrote is rewritten",
            path,
            tableName,
            snapshotId,
            MetadataColumns.ROW_ID.name(),
            MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name()));
  }

  /**
   * The data files of one partition of a snapshot, planned once per snapshot and partition and
   * cached, so a rewrite of many base files plans each partition once instead of the table per
   * file.
   */
  private static Map<String, BaseFileLineage> lineageInPartition(
      Table table,
      TableMetadata metadata,
      Snapshot snapshot,
      String partitionPath,
      Expression partitionFilter) {
    String key = metadata.uuid() + "/" + snapshot.snapshotId() + "/" + partitionPath;
    Map<String, BaseFileLineage> cached = LINEAGE_BY_PARTITION.get(key);
    if (cached != null) {
      return cached;
    }
    Map<String, BaseFileLineage> lineageByPath = new HashMap<>();
    try (CloseableIterable<FileScanTask> tasks =
        table.newScan().useSnapshot(snapshot.snapshotId()).filter(partitionFilter).planFiles()) {
      for (FileScanTask task : tasks) {
        lineageByPath.put(
            task.file().location(),
            new BaseFileLineage(task.file().firstRowId(), task.file().dataSequenceNumber()));
      }
    } catch (IOException e) {
      throw new ReadException("Failed to plan the data files of Iceberg snapshot " + snapshot, e);
    }
    LINEAGE_BY_PARTITION.put(key, lineageByPath);
    return lineageByPath;
  }

  private static Expression partitionFilter(
      TableMetadata metadata, HoodieTableMetaClient metaClient, String partitionPath) {
    if (!metadata.spec().isPartitioned()) {
      return Expressions.alwaysTrue();
    }
    PathBasedPartitionSpecExtractor specExtractor =
        IcebergTableFormat.sourcePartitionSpecExtractor(metaClient.getTableConfig());
    List<InternalPartitionField> partitionFields =
        specExtractor.spec(IcebergSchemaExtractor.getInstance().fromIceberg(metadata.schema()));
    List<PartitionValue> partitionValues =
        new PathBasedPartitionValuesExtractor(specExtractor.getPathToPartitionFieldFormat())
            .extractPartitionValues(partitionFields, partitionPath);
    return IcebergPartitionValueConverter.getInstance()
        .partitionFilter(
            metadata.spec(), metadata.schema(), Collections.singletonList(partitionValues));
  }

  /**
   * Fails unless every base file the instant rewrote into an Iceberg table at format version 3
   * stores both row lineage columns where Iceberg reads them, which a Hudi writer that does not
   * support the columns would not do: the base files a clustering writes, and those that replace an
   * earlier base file of their file group. Called before the instant is recorded in Iceberg, so a
   * failure leaves it unpublished.
   *
   * @param declared whether the table config makes the format declare the row lineage columns
   * @throws HoodieException naming the base files without the columns
   */
  static void requireRowLineageInRewrittenBaseFiles(
      HoodieCommitMetadata commitMetadata,
      HoodieInstant instant,
      HoodieEngineContext engineContext,
      HoodieTableMetaClient metaClient,
      boolean declared) {
    boolean clustering = commitMetadata.getOperationType() == WriteOperationType.CLUSTER;
    List<String> rewrittenBaseFiles = new ArrayList<>();
    for (HoodieWriteStat writeStat : commitMetadata.getWriteStats()) {
      if (writeStat.getPath() == null) {
        continue;
      }
      StoragePath path = new StoragePath(metaClient.getBasePath(), writeStat.getPath());
      boolean replacesBaseFile =
          writeStat.getPrevBaseFile() != null && !writeStat.getPrevBaseFile().isEmpty();
      if (FSUtils.isBaseFile(path) && (clustering || replacesBaseFile)) {
        rewrittenBaseFiles.add(path.toString());
      }
    }
    if (rewrittenBaseFiles.isEmpty()) {
      return;
    }
    Table table = cachedTable(metaClient);
    int formatVersion = refresh(metaClient, table).formatVersion();
    if (formatVersion < 3) {
      return;
    }
    if (!declared) {
      throw new HoodieException(
          String.format(
              "Instant %s of table %s rewrote base files without the Iceberg row lineage columns,"
                  + " since its table config sets Iceberg format version 2, but its Iceberg table"
                  + " is at format version %d; the instant is not recorded in Iceberg. Set %s=%d"
                  + " in hoodie.properties",
              instant,
              metaClient.getTableConfig().getTableName(),
              formatVersion,
              IcebergFormatConfig.FORMAT_VERSION,
              formatVersion));
    }
    StorageConfiguration<?> storageConf = metaClient.getStorageConf();
    List<String> problems =
        engineContext
            .map(
                rewrittenBaseFiles,
                path -> missingRowLineage(storageConf, path),
                Math.min(rewrittenBaseFiles.size(), 100))
            .stream()
            .filter(problem -> !problem.isEmpty())
            .collect(Collectors.toList());
    if (!problems.isEmpty()) {
      throw new HoodieException(
          String.format(
              "Instant %s of table %s rewrote base files without the Iceberg row lineage columns"
                  + " (%s, %s) that format version 3 requires, so Iceberg would give the rows they"
                  + " carry over new row ids; the instant is not recorded in Iceberg. Write the"
                  + " table with a Hudi version that stores the columns. %s",
              instant,
              metaClient.getTableConfig().getTableName(),
              MetadataColumns.ROW_ID.name(),
              MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name(),
              String.join("; ", problems)));
    }
  }

  /**
   * Why the Parquet schema of a base file does not carry the row lineage columns, or an empty
   * string when it does. A file with field ids is read by field id and not through the name
   * mapping, so each column then also needs its reserved id.
   */
  static String missingRowLineage(StorageConfiguration<?> storageConf, String path) {
    StoragePath filePath = new StoragePath(path);
    if (!path.endsWith(".parquet")) {
      return path + " is not a Parquet file";
    }
    MessageType schema =
        ParquetUtils.readMetadata(HoodieStorageUtils.getStorage(filePath, storageConf), filePath)
            .getFileMetaData()
            .getSchema();
    boolean hasFieldIds = hasFieldIds(schema);
    List<String> problems = new ArrayList<>();
    for (Types.NestedField column : COLUMNS) {
      Type field = schema.containsField(column.name()) ? schema.getType(column.name()) : null;
      if (field == null
          || !field.isPrimitive()
          || field.asPrimitiveType().getPrimitiveTypeName() != PrimitiveType.PrimitiveTypeName.INT64
          || field.isRepetition(Type.Repetition.REPEATED)) {
        problems.add("no " + column.name() + " column of type long");
      } else if (hasFieldIds
          && (field.getId() == null || field.getId().intValue() != column.fieldId())) {
        problems.add(
            column.name()
                + " has field id "
                + field.getId()
                + " instead of "
                + column.fieldId()
                + " in a file with field ids");
      }
    }
    return problems.isEmpty() ? "" : path + ": " + String.join(", ", problems);
  }

  private static boolean hasFieldIds(Type type) {
    if (type.getId() != null) {
      return true;
    }
    return !type.isPrimitive()
        && type.asGroupType().getFields().stream().anyMatch(IcebergRowLineage::hasFieldIds);
  }
}
