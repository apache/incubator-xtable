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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;

/** The empty placeholder schema and spec a created table starts with, kept or removed. */
public class TestIcebergTableManagerPlaceholderSchema {
  private static final Namespace NAMESPACE = Namespace.of("database1");

  // Field IDs in depth-first order (a struct's fields follow it), unlike the order Iceberg assigns
  // when it renumbers a schema, so a renumbering would show up.
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "key", Types.StringType.get()),
          Types.NestedField.optional(
              2,
              "location",
              Types.StructType.of(
                  Types.NestedField.optional(3, "lat", Types.DoubleType.get()),
                  Types.NestedField.optional(4, "lon", Types.DoubleType.get()))),
          Types.NestedField.optional(5, "region", Types.StringType.get()));

  @TempDir Path tempDir;

  /** An in-memory catalog that has the test namespace. */
  public static class CatalogWithNamespace extends InMemoryCatalog {
    @Override
    public void initialize(String name, Map<String, String> properties) {
      super.initialize(name, properties);
      createNamespace(NAMESPACE);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void placeholderSchemaRemovedWhenConfigured(boolean throughCatalog) {
    Table table = create(throughCatalog, true, partitionedByRegion());
    TableMetadata metadata = ((BaseTable) table).operations().current();

    assertEquals(1, metadata.schemas().size(), "only the real schema is left");
    assertTrue(metadata.schemas().stream().noneMatch(s -> s.columns().isEmpty()));
    assertEquals(SCHEMA.asStruct(), table.schema().asStruct(), "field IDs are kept");
    assertEquals(
        "region", table.spec().fields().get(0).name(), "the partition spec is the given one");
    // The unpartitioned placeholder spec stays: some REST catalogs reject removing a spec.
    assertEquals(2, metadata.specs().size());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void unpartitionedTableKeepsItsOnlySpec(boolean throughCatalog) {
    Table table = create(throughCatalog, true, PartitionSpec.unpartitioned());
    TableMetadata metadata = ((BaseTable) table).operations().current();

    assertEquals(1, metadata.schemas().size());
    assertEquals(SCHEMA.asStruct(), table.schema().asStruct());
    assertEquals(1, metadata.specs().size());
    assertTrue(table.spec().isUnpartitioned());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void placeholderSchemaKeptByDefault(boolean throughCatalog) {
    Table table = create(throughCatalog, false, partitionedByRegion());
    TableMetadata metadata = ((BaseTable) table).operations().current();

    assertEquals(
        2,
        metadata.schemas().size(),
        metadata.schemas().stream().map(Schema::toString).collect(Collectors.joining()));
    assertTrue(metadata.schemas().stream().anyMatch(s -> s.columns().isEmpty()));
    assertEquals(SCHEMA.asStruct(), table.schema().asStruct());
  }

  private static PartitionSpec partitionedByRegion() {
    return PartitionSpec.builderFor(SCHEMA).identity("region").build();
  }

  private Table create(boolean throughCatalog, boolean removePlaceholder, PartitionSpec spec) {
    Configuration conf = new Configuration();
    conf.setBoolean(IcebergTableManager.ICEBERG_REMOVE_PLACEHOLDER_SCHEMA, removePlaceholder);
    String name = "t_" + UUID.randomUUID().toString().replace("-", "");
    String basePath = tempDir.resolve(name).toUri().toString();
    IcebergCatalogConfig catalogConfig =
        throughCatalog
            ? IcebergCatalogConfig.builder()
                .catalogImpl(CatalogWithNamespace.class.getName())
                .catalogName("in_memory_" + name)
                .build()
            : null;
    return IcebergTableManager.of(conf)
        .getOrCreateTable(
            catalogConfig, TableIdentifier.of(NAMESPACE, name), basePath, SCHEMA, spec);
  }
}
