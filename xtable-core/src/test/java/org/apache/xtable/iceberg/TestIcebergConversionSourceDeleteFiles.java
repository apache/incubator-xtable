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
 
package org.apache.xtable.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.model.InternalSnapshot;
import org.apache.xtable.model.TableChange;
import org.apache.xtable.model.storage.PartitionFileGroup;
import org.apache.xtable.model.storage.TableFormat;

/**
 * Row-level delete files cannot be represented in the conversion targets, so the Iceberg source
 * must fail instead of emitting the data files as if every row in them were still live.
 */
class TestIcebergConversionSourceDeleteFiles {
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "name", Types.StringType.get()));

  /** The kinds of row-level delete files an Iceberg writer can commit. */
  enum DeleteKind {
    POSITION_DELETE("a position delete file", 2),
    EQUALITY_DELETE("an equality delete file", 2),
    DELETION_VECTOR("a deletion vector", 3);

    private final String description;
    private final int formatVersion;

    DeleteKind(String description, int formatVersion) {
      this.description = description;
      this.formatVersion = formatVersion;
    }
  }

  @TempDir private Path tempDir;
  private Configuration hadoopConf;

  @BeforeEach
  void setUp() {
    hadoopConf = new Configuration();
    hadoopConf.set("fs.defaultFS", "file:///");
  }

  @ParameterizedTest
  @EnumSource(DeleteKind.class)
  void fullSyncFailsWhenDeleteFilesApplyToTheSnapshot(DeleteKind kind) throws IOException {
    Table table = createTable(kind.formatVersion);
    DataFile dataFile = writeDataFile(table, 1, 2, 3);
    table.newAppend().appendFile(dataFile).commit();
    table.newRowDelta().addDeletes(deleteFile(table, kind, dataFile)).commit();
    Snapshot deleteSnapshot = table.currentSnapshot();

    NotSupportedException exception =
        assertThrows(
            NotSupportedException.class, () -> getConversionSource(table).getCurrentSnapshot());

    assertTrue(
        exception.getMessage().contains("snapshot " + deleteSnapshot.snapshotId()),
        exception.getMessage());
    assertTrue(
        exception.getMessage().contains("delete files apply to data file " + dataFile.location()),
        exception.getMessage());
    assertTrue(
        exception
            .getMessage()
            .contains(IcebergConversionSourceConfig.ALLOW_UNSUPPORTED_DELETE_FILES + "=true"),
        exception.getMessage());
  }

  @ParameterizedTest
  @EnumSource(DeleteKind.class)
  void incrementalSyncFailsOnTheCommitThatAddsDeleteFiles(DeleteKind kind) throws IOException {
    Table table = createTable(kind.formatVersion);
    DataFile firstFile = writeDataFile(table, 1, 2, 3);
    table.newAppend().appendFile(firstFile).commit();
    Snapshot appendSnapshot = table.currentSnapshot();
    DeleteFile deleteFile = deleteFile(table, kind, firstFile);
    table.newRowDelta().addDeletes(deleteFile).commit();
    Snapshot deleteSnapshot = table.currentSnapshot();
    table.newAppend().appendFile(writeDataFile(table, 4, 5)).commit();
    Snapshot laterAppendSnapshot = table.currentSnapshot();

    IcebergConversionSource conversionSource = getConversionSource(table);
    assertEquals(
        1,
        conversionSource
            .getTableChangeForCommit(appendSnapshot)
            .getFilesDiff()
            .dataFilesAdded()
            .size());
    NotSupportedException exception =
        assertThrows(
            NotSupportedException.class,
            () -> conversionSource.getTableChangeForCommit(deleteSnapshot));
    assertTrue(
        exception.getMessage().contains("snapshot " + deleteSnapshot.snapshotId()),
        exception.getMessage());
    assertTrue(
        exception
            .getMessage()
            .contains("the commit adds " + kind.description + " " + deleteFile.location()),
        exception.getMessage());
    // A commit that adds no delete files is still converted.
    assertEquals(
        1,
        conversionSource
            .getTableChangeForCommit(laterAppendSnapshot)
            .getFilesDiff()
            .dataFilesAdded()
            .size());
  }

  @Test
  void incrementalSyncFailsWhenACommitAddsDataAndDeleteFilesTogether() throws IOException {
    // Upserts from engines such as Flink add the new rows and the equality deletes in one commit.
    Table table = createTable(2);
    DataFile firstFile = writeDataFile(table, 1, 2, 3);
    table.newAppend().appendFile(firstFile).commit();
    table
        .newRowDelta()
        .addRows(writeDataFile(table, 2))
        .addDeletes(deleteFile(table, DeleteKind.EQUALITY_DELETE, firstFile))
        .commit();
    Snapshot upsertSnapshot = table.currentSnapshot();

    assertThrows(
        NotSupportedException.class,
        () -> getConversionSource(table).getTableChangeForCommit(upsertSnapshot));
  }

  @Test
  void deleteFilesThatNoLongerApplyDoNotFailTheSync() throws IOException {
    Table table = createTable(2);
    DataFile originalFile = writeDataFile(table, 1, 2, 3);
    table.newAppend().appendFile(originalFile).commit();
    table
        .newRowDelta()
        .addDeletes(deleteFile(table, DeleteKind.POSITION_DELETE, originalFile))
        .commit();
    Snapshot deleteSnapshot = table.currentSnapshot();
    // Rewrite the data file with the delete applied, as compaction does. The delete file stays in
    // the table metadata but no longer applies, because the new data file has a higher sequence
    // number.
    DataFile rewrittenFile = writeDataFile(table, 2, 3);
    table
        .newRewrite()
        .validateFromSnapshot(deleteSnapshot.snapshotId())
        .deleteFile(originalFile)
        .addFile(rewrittenFile)
        .commit();
    Snapshot rewriteSnapshot = table.currentSnapshot();
    assertEquals("1", rewriteSnapshot.summary().get("total-delete-files"));

    IcebergConversionSource conversionSource = getConversionSource(table);
    InternalSnapshot internalSnapshot = conversionSource.getCurrentSnapshot();
    assertEquals(
        Collections.singletonList(fileName(rewrittenFile.location())),
        dataFileNames(internalSnapshot));
    TableChange rewriteChange = conversionSource.getTableChangeForCommit(rewriteSnapshot);
    assertEquals(1, rewriteChange.getFilesDiff().dataFilesAdded().size());
    assertEquals(1, rewriteChange.getFilesDiff().dataFilesRemoved().size());
  }

  @ParameterizedTest
  @EnumSource(DeleteKind.class)
  void deleteFilesAreIgnoredWhenAllowed(DeleteKind kind) throws IOException {
    Table table = createTable(kind.formatVersion);
    DataFile dataFile = writeDataFile(table, 1, 2, 3);
    table.newAppend().appendFile(dataFile).commit();
    table.newRowDelta().addDeletes(deleteFile(table, kind, dataFile)).commit();
    Snapshot deleteSnapshot = table.currentSnapshot();
    Properties properties = new Properties();
    properties.setProperty(IcebergConversionSourceConfig.ALLOW_UNSUPPORTED_DELETE_FILES, "true");

    IcebergConversionSource conversionSource = getConversionSource(table, properties);
    assertEquals(
        Collections.singletonList(fileName(dataFile.location())),
        dataFileNames(conversionSource.getCurrentSnapshot()));
    TableChange deleteChange = conversionSource.getTableChangeForCommit(deleteSnapshot);
    assertTrue(deleteChange.getFilesDiff().dataFilesAdded().isEmpty());
    assertTrue(deleteChange.getFilesDiff().dataFilesRemoved().isEmpty());
  }

  private Table createTable(int formatVersion) {
    return new HadoopTables(hadoopConf)
        .create(
            SCHEMA,
            PartitionSpec.unpartitioned(),
            Collections.singletonMap(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion)),
            tempDir.resolve("table").toString());
  }

  private IcebergConversionSource getConversionSource(Table table) {
    return getConversionSource(table, new Properties());
  }

  private IcebergConversionSource getConversionSource(Table table, Properties properties) {
    SourceTable sourceTable =
        SourceTable.builder()
            .name(table.name())
            .basePath(table.location())
            .formatName(TableFormat.ICEBERG)
            .additionalProperties(properties)
            .build();
    return IcebergConversionSource.builder()
        .hadoopConf(hadoopConf)
        .sourceTableConfig(sourceTable)
        .build();
  }

  private static DataFile writeDataFile(Table table, long... ids) throws IOException {
    String path = table.locationProvider().newDataLocation(UUID.randomUUID() + ".parquet");
    DataWriter<GenericRecord> writer =
        Parquet.writeData(table.io().newOutputFile(path))
            .schema(SCHEMA)
            .createWriterFunc(GenericParquetWriter::create)
            .overwrite()
            .withSpec(table.spec())
            .build();
    try {
      for (long id : ids) {
        GenericRecord record = GenericRecord.create(SCHEMA);
        record.setField("id", id);
        record.setField("name", "name-" + id);
        writer.write(record);
      }
    } finally {
      writer.close();
    }
    return writer.toDataFile();
  }

  /**
   * Builds the metadata of a delete file. The source only reads delete file metadata from the
   * manifests, so the file itself is not written.
   */
  private static DeleteFile deleteFile(Table table, DeleteKind kind, DataFile dataFile) {
    FileMetadata.Builder builder =
        FileMetadata.deleteFileBuilder(table.spec()).withFileSizeInBytes(64).withRecordCount(1);
    switch (kind) {
      case POSITION_DELETE:
        return builder
            .ofPositionDeletes()
            .withFormat(FileFormat.PARQUET)
            .withPath(table.locationProvider().newDataLocation(UUID.randomUUID() + "-pos.parquet"))
            .build();
      case EQUALITY_DELETE:
        return builder
            .ofEqualityDeletes(SCHEMA.findField("id").fieldId())
            .withFormat(FileFormat.PARQUET)
            .withPath(table.locationProvider().newDataLocation(UUID.randomUUID() + "-eq.parquet"))
            .build();
      case DELETION_VECTOR:
        return builder
            .ofPositionDeletes()
            .withFormat(FileFormat.PUFFIN)
            .withPath(table.locationProvider().newDataLocation(UUID.randomUUID() + "-dv.puffin"))
            .withReferencedDataFile(dataFile.location())
            .withContentOffset(4)
            .withContentSizeInBytes(40)
            .build();
      default:
        throw new IllegalArgumentException("Unknown delete kind " + kind);
    }
  }

  private static List<String> dataFileNames(InternalSnapshot snapshot) {
    return snapshot.getPartitionedDataFiles().stream()
        .map(PartitionFileGroup::getDataFiles)
        .flatMap(List::stream)
        .map(dataFile -> fileName(dataFile.getPhysicalPath()))
        .collect(Collectors.toList());
  }

  private static String fileName(String path) {
    return new org.apache.hadoop.fs.Path(path).getName();
  }
}
