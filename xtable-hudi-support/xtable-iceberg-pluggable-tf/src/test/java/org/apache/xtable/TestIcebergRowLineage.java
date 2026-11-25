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

import static org.apache.hudi.hadoop.fs.HadoopFSUtils.getStorageConf;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.hudi.common.FormatMetaColumn;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.storage.StorageConfiguration;

import org.apache.iceberg.MetadataColumns;

/**
 * The row lineage columns the format declares, and the check that a rewritten base file stores them
 * where Iceberg reads them.
 */
class TestIcebergRowLineage {
  private static final int ROW_ID = MetadataColumns.ROW_ID.fieldId();
  private static final int SEQUENCE_NUMBER = MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId();
  private static final StorageConfiguration<?> STORAGE_CONF = getStorageConf(new Configuration());

  @TempDir Path tempDir;

  @Test
  void declaresTheRowLineageColumnsAtFormatVersionThreeOnly() throws Exception {
    assertEquals(
        Arrays.asList(
            MetadataColumns.ROW_ID.name(), MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name()),
        names(declaredColumns(null)));
    assertEquals(
        Arrays.asList(
            MetadataColumns.ROW_ID.name(), MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name()),
        names(declaredColumns("3")));
    assertEquals(0, declaredColumns("2").size());

    for (FormatMetaColumn column : declaredColumns("3")) {
      HoodieSchemaField field = column.field();
      assertEquals(HoodieSchemaType.LONG, field.schema().getNonNullType().getType());
      assertEquals(true, field.isNullable());
      assertNull(column.valueOnWrite("20260101000000000"));
      // The format and its columns travel to the tasks of a distributed write.
      assertEquals(column.field().name(), roundTrip(column).field().name());
    }
  }

  @Test
  void anUpdatedRowKeepsTheRowIdOfTheRowItReplacesAndGetsTheSequenceNumberOfTheRewrite() {
    List<FormatMetaColumn> columns = declaredColumns("3");
    assertEquals(7L, columns.get(0).valueOnUpdate("20260101000000000", 7L));
    assertNull(columns.get(0).valueOnUpdate("20260101000000000", null));
    assertNull(columns.get(1).valueOnUpdate("20260101000000000", 3L));
  }

  @Test
  void acceptsABaseFileWithTheRowLineageColumns() throws IOException {
    assertEquals(
        "",
        IcebergRowLineage.missingRowLineage(
            STORAGE_CONF, write("plain.parquet", tableColumns(false), lineageColumns(null))));
    assertEquals(
        "",
        IcebergRowLineage.missingRowLineage(
            STORAGE_CONF,
            write(
                "ids.parquet",
                tableColumns(true),
                lineageColumns(new int[] {ROW_ID, SEQUENCE_NUMBER}))));
  }

  @Test
  void refusesABaseFileWithoutTheRowLineageColumnsWhereIcebergReadsThem() throws IOException {
    String withoutColumns = write("none.parquet", tableColumns(false));
    assertEquals(
        withoutColumns
            + ": no _row_id column of type long, no _last_updated_sequence_number column of type"
            + " long",
        IcebergRowLineage.missingRowLineage(STORAGE_CONF, withoutColumns));

    // Iceberg ignores the name mapping for a file with field ids, so the columns need their ids.
    String withoutIds = write("noids.parquet", tableColumns(true), lineageColumns(null));
    assertEquals(
        withoutIds
            + ": _row_id has field id null instead of 2147483540 in a file with field ids,"
            + " _last_updated_sequence_number has field id null instead of 2147483539 in a file"
            + " with field ids",
        IcebergRowLineage.missingRowLineage(STORAGE_CONF, withoutIds));

    String wrongType =
        write(
            "wrongtype.parquet",
            tableColumns(false),
            Arrays.asList(
                Types.optional(INT32).named(MetadataColumns.ROW_ID.name()),
                Types.optional(INT64).named(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name())));
    assertEquals(
        wrongType + ": no _row_id column of type long",
        IcebergRowLineage.missingRowLineage(STORAGE_CONF, wrongType));
  }

  private static List<FormatMetaColumn> declaredColumns(String formatVersion) {
    Properties properties = new Properties();
    properties.setProperty("hoodie.table.name", "lineage");
    properties.setProperty("hoodie.table.version", "8");
    if (formatVersion != null) {
      properties.setProperty(IcebergFormatConfig.FORMAT_VERSION, formatVersion);
    }
    IcebergTableFormat format = new IcebergTableFormat();
    format.init(properties);
    return format.getFormatMetaColumns();
  }

  private static List<String> names(List<FormatMetaColumn> columns) {
    return columns.stream().map(column -> column.field().name()).collect(Collectors.toList());
  }

  private static FormatMetaColumn roundTrip(FormatMetaColumn column) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(column);
    }
    try (ObjectInputStream in =
        new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      return (FormatMetaColumn) in.readObject();
    }
  }

  private static List<org.apache.parquet.schema.Type> tableColumns(boolean withIds) {
    org.apache.parquet.schema.Type key = Types.required(BINARY).named("key");
    org.apache.parquet.schema.Type value = Types.optional(INT64).named("value");
    return withIds ? Arrays.asList(key.withId(1), value.withId(2)) : Arrays.asList(key, value);
  }

  private static List<org.apache.parquet.schema.Type> lineageColumns(int[] ids) {
    org.apache.parquet.schema.Type rowId =
        Types.optional(INT64).named(MetadataColumns.ROW_ID.name());
    org.apache.parquet.schema.Type sequenceNumber =
        Types.optional(INT64).named(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name());
    return ids == null
        ? Arrays.asList(rowId, sequenceNumber)
        : Arrays.asList(rowId.withId(ids[0]), sequenceNumber.withId(ids[1]));
  }

  @SafeVarargs
  private final String write(String name, List<org.apache.parquet.schema.Type>... columnGroups)
      throws IOException {
    MessageType schema =
        new MessageType(
            "table",
            Arrays.stream(columnGroups).flatMap(List::stream).collect(Collectors.toList()));
    String path = tempDir.resolve(name).toUri().toString();
    try (ParquetWriter<?> writer =
        ExampleParquetWriter.builder(new org.apache.hadoop.fs.Path(path))
            .withType(schema)
            .build()) {
      // A footer without rows is all the check reads.
    }
    return path;
  }
}
