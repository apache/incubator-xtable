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

import java.nio.file.Path;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;

/** Verifies the Iceberg table format version written when creating a table. */
public class TestIcebergFormatVersion {
  private static final TableIdentifier IDENTIFIER = TableIdentifier.of("db", "table");
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

  @TempDir Path tempDir;

  @Test
  void defaultsToFormatVersionTwo() {
    Table table = createTable(new Configuration(), "default");
    assertEquals(2, formatVersion(table));
  }

  @Test
  void honorsConfiguredFormatVersionThree() {
    Configuration conf = new Configuration();
    conf.setInt(IcebergTableManager.ICEBERG_FORMAT_VERSION, 3);
    Table table = createTable(conf, "v3");
    assertEquals(3, formatVersion(table));
  }

  private Table createTable(Configuration conf, String subdir) {
    String basePath = "file://" + tempDir.resolve(subdir);
    return IcebergTableManager.of(conf)
        .getOrCreateTable(null, IDENTIFIER, basePath, SCHEMA, PartitionSpec.unpartitioned());
  }

  private static int formatVersion(Table table) {
    return ((BaseTable) table).operations().current().formatVersion();
  }
}
