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
 
package org.apache.xtable.spark.extensions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.connector.catalog.Identifier;
import org.junit.jupiter.api.Test;

import scala.Option;
import scala.collection.JavaConverters;

import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.iceberg.IcebergCatalogConfig;

public class TestXTableIndex {

  @Test
  void loadsTableFromTheSparkCatalogOptions() {
    Map<String, String> sparkConf = new HashMap<>();
    sparkConf.put("spark.sql.catalog.cat", "org.apache.iceberg.spark.SparkCatalog");
    sparkConf.put("spark.sql.catalog.cat.type", "hadoop");
    sparkConf.put("spark.sql.catalog.cat.warehouse", "file:///warehouse");
    sparkConf.put("spark.sql.catalog.other.type", "rest");

    SourceTable sourceTable =
        XTableIndex.toSourceTable(
            "cat.db.orders",
            "file:///warehouse/db/orders",
            "file:///warehouse/db/orders/data",
            Option.apply("cat"),
            Option.apply(Identifier.of(new String[] {"db"}, "orders")),
            toScala(sparkConf));

    assertEquals("orders", sourceTable.getName());
    assertEquals(Arrays.asList("db"), Arrays.asList(sourceTable.getNamespace()));
    assertEquals("file:///warehouse/db/orders", sourceTable.getBasePath());
    assertEquals("file:///warehouse/db/orders/data", sourceTable.getDataPath());
    IcebergCatalogConfig catalogConfig = (IcebergCatalogConfig) sourceTable.getCatalogConfig();
    assertEquals("cat", catalogConfig.getCatalogName());
    assertEquals("org.apache.iceberg.hadoop.HadoopCatalog", catalogConfig.getCatalogImpl());
    Map<String, String> expectedOptions = new HashMap<>();
    expectedOptions.put("type", "hadoop");
    expectedOptions.put("warehouse", "file:///warehouse");
    assertEquals(expectedOptions, catalogConfig.getCatalogOptions());
  }

  @Test
  void resolvesTheCatalogImplementationLikeIceberg() {
    assertEquals("org.apache.iceberg.hive.HiveCatalog", catalogImpl(new HashMap<>()));
    Map<String, String> glue = new HashMap<>();
    glue.put("spark.sql.catalog.cat.type", "glue");
    assertEquals("org.apache.iceberg.aws.glue.GlueCatalog", catalogImpl(glue));
    Map<String, String> custom = new HashMap<>();
    custom.put("spark.sql.catalog.cat.type", "rest");
    custom.put("spark.sql.catalog.cat.catalog-impl", "com.example.CustomCatalog");
    assertEquals("com.example.CustomCatalog", catalogImpl(custom));
    Map<String, String> unknown = new HashMap<>();
    unknown.put("spark.sql.catalog.cat.type", "unknown");
    assertThrows(IllegalArgumentException.class, () -> catalogImpl(unknown));
  }

  @Test
  void loadsTableReadByPathFromItsLocation() {
    SourceTable sourceTable =
        XTableIndex.toSourceTable(
            "file:///tables/orders",
            "file:///tables/orders",
            "file:///tables/orders/data",
            Option.empty(),
            Option.empty(),
            toScala(new HashMap<>()));
    assertEquals("file:///tables/orders", sourceTable.getBasePath());
    assertEquals("file:///tables/orders/data", sourceTable.getDataPath());
    assertNull(sourceTable.getCatalogConfig());
  }

  private static String catalogImpl(Map<String, String> sparkConf) {
    SourceTable sourceTable =
        XTableIndex.toSourceTable(
            "cat.db.orders",
            "file:///warehouse/db/orders",
            "file:///warehouse/db/orders/data",
            Option.apply("cat"),
            Option.apply(Identifier.of(new String[] {"db"}, "orders")),
            toScala(sparkConf));
    return ((IcebergCatalogConfig) sourceTable.getCatalogConfig()).getCatalogImpl();
  }

  private static scala.collection.immutable.Map<String, String> toScala(Map<String, String> map) {
    return scala.collection.immutable.Map$.MODULE$
        .<String, String>empty()
        .$plus$plus(JavaConverters.mapAsScalaMap(map));
  }
}
