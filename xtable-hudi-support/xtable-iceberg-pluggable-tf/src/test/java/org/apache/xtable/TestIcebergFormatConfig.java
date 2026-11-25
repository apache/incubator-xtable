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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

import org.apache.xtable.iceberg.IcebergCatalogConfig;
import org.apache.xtable.iceberg.IcebergTableManager;

class TestIcebergFormatConfig {

  private static Configuration hadoopConfWithCatalog() {
    Configuration conf = new Configuration(false);
    conf.set(IcebergFormatConfig.HADOOP_CATALOG_NAME, "hadoop_catalog");
    conf.set(IcebergFormatConfig.HADOOP_CATALOG_IMPL, "org.example.HadoopConfCatalog");
    conf.set(IcebergFormatConfig.HADOOP_CATALOG_OPTION_PREFIX + "uri", "thrift://hadoop:9083");
    return conf;
  }

  @Test
  void withoutACatalogTheTableIsPathBased() {
    IcebergFormatConfig config = IcebergFormatConfig.of(new Properties(), new Configuration(false));
    assertNull(config.getCatalogConfig());
    assertNull(config.getNamespace());
    assertEquals(TableIdentifier.of("t"), config.tableIdentifier("t"));

    assertNull(IcebergFormatConfig.of(new Properties(), null).getCatalogConfig());
  }

  @Test
  void collectsTheCatalogFromTheTableConfig() {
    Properties tableConfig = new Properties();
    tableConfig.setProperty(IcebergFormatConfig.CATALOG_NAME, "table_catalog");
    tableConfig.setProperty(IcebergFormatConfig.CATALOG_IMPL, "org.example.TableCatalog");
    tableConfig.setProperty(IcebergFormatConfig.CATALOG_OPTION_PREFIX + "uri", "thrift://t:9083");
    tableConfig.setProperty(IcebergFormatConfig.CATALOG_OPTION_PREFIX + "warehouse", "/wh");
    tableConfig.setProperty("hoodie.table.name", "t");

    Map<String, String> expectedOptions = new HashMap<>();
    expectedOptions.put("uri", "thrift://t:9083");
    expectedOptions.put("warehouse", "/wh");
    assertEquals(
        IcebergCatalogConfig.builder()
            .catalogName("table_catalog")
            .catalogImpl("org.example.TableCatalog")
            .catalogOptions(expectedOptions)
            .build(),
        IcebergFormatConfig.of(tableConfig, null).getCatalogConfig());
  }

  @Test
  void theTableConfigWinsOverTheHadoopConf() {
    Properties tableConfig = new Properties();
    tableConfig.setProperty(IcebergFormatConfig.CATALOG_IMPL, "org.example.TableCatalog");

    IcebergCatalogConfig catalog =
        IcebergFormatConfig.of(tableConfig, hadoopConfWithCatalog()).getCatalogConfig();
    assertEquals(
        IcebergCatalogConfig.builder()
            .catalogName(IcebergFormatConfig.DEFAULT_CATALOG_NAME)
            .catalogImpl("org.example.TableCatalog")
            .build(),
        catalog,
        "no option or name may leak in from the Hadoop configuration");
  }

  @Test
  void fallsBackToTheHadoopConfWhenTheTableConfigNamesNoCatalog() {
    Map<String, String> expectedOptions = new HashMap<>();
    expectedOptions.put("uri", "thrift://hadoop:9083");
    assertEquals(
        IcebergCatalogConfig.builder()
            .catalogName("hadoop_catalog")
            .catalogImpl("org.example.HadoopConfCatalog")
            .catalogOptions(expectedOptions)
            .build(),
        IcebergFormatConfig.of(new Properties(), hadoopConfWithCatalog()).getCatalogConfig());

    Configuration withoutName = hadoopConfWithCatalog();
    withoutName.unset(IcebergFormatConfig.HADOOP_CATALOG_NAME);
    assertEquals(
        IcebergFormatConfig.DEFAULT_CATALOG_NAME,
        IcebergFormatConfig.of(new Properties(), withoutName).getCatalogConfig().getCatalogName());
  }

  @Test
  void aHadoopConfCatalogWithoutAnImplementationIsIgnored() {
    Configuration conf = new Configuration(false);
    conf.set(IcebergFormatConfig.HADOOP_CATALOG_NAME, "hadoop_catalog");
    assertNull(IcebergFormatConfig.of(new Properties(), conf).getCatalogConfig());
  }

  @Test
  void refusesACatalogNameWithoutAnImplementation() {
    Properties tableConfig = new Properties();
    tableConfig.setProperty(IcebergFormatConfig.CATALOG_NAME, "table_catalog");
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () -> IcebergFormatConfig.of(tableConfig, hadoopConfWithCatalog()));
    assertEquals(
        IcebergFormatConfig.CATALOG_NAME
            + " is set to table_catalog but "
            + IcebergFormatConfig.CATALOG_IMPL
            + " is not set",
        error.getMessage());
  }

  @Test
  void parsesADotSeparatedNamespace() {
    Properties tableConfig = new Properties();
    tableConfig.setProperty(IcebergFormatConfig.NAMESPACE, "db");
    IcebergFormatConfig single = IcebergFormatConfig.of(tableConfig, null);
    assertArrayEquals(new String[] {"db"}, single.getNamespace());
    assertEquals(TableIdentifier.of(Namespace.of("db"), "t"), single.tableIdentifier("t"));

    tableConfig.setProperty(IcebergFormatConfig.NAMESPACE, "a.b.c");
    IcebergFormatConfig nested = IcebergFormatConfig.of(tableConfig, null);
    assertArrayEquals(new String[] {"a", "b", "c"}, nested.getNamespace());
    assertEquals(TableIdentifier.of(Namespace.of("a", "b", "c"), "t"), nested.tableIdentifier("t"));

    tableConfig.setProperty(IcebergFormatConfig.NAMESPACE, "");
    assertNull(IcebergFormatConfig.of(tableConfig, null).getNamespace());
  }

  @Test
  void initRefusesInvalidSettingsBeforeAnyInstantExists() {
    Properties tableConfig = new Properties();
    tableConfig.setProperty(IcebergFormatConfig.CATALOG_NAME, "table_catalog");
    assertThrows(IllegalArgumentException.class, () -> new IcebergTableFormat().init(tableConfig));

    for (String unsupportedVersion : new String[] {"three", "1", "4"}) {
      Properties badVersion = new Properties();
      badVersion.setProperty(IcebergFormatConfig.FORMAT_VERSION, unsupportedVersion);
      IllegalArgumentException refusal =
          assertThrows(
              IllegalArgumentException.class, () -> new IcebergTableFormat().init(badVersion));
      assertTrue(
          refusal.getMessage().contains("must be 2 or 3"),
          unsupportedVersion + ": " + refusal.getMessage());
    }
  }

  @Test
  void formatVersionDefaultsToThree() {
    assertEquals(3, IcebergFormatConfig.of(new Properties(), null).getFormatVersion());
    assertEquals(
        3, IcebergFormatConfig.of(new Properties(), new Configuration(false)).getFormatVersion());
  }

  @Test
  void formatVersionFallsBackToTheHadoopConfWhenSetThere() {
    Configuration conf = new Configuration(false);
    conf.set(IcebergTableManager.ICEBERG_FORMAT_VERSION, "2");
    assertEquals(2, IcebergFormatConfig.of(new Properties(), conf).getFormatVersion());
  }

  @Test
  void formatVersionFromTheTableConfigWinsOverTheHadoopConf() {
    Configuration conf = new Configuration(false);
    conf.set(IcebergTableManager.ICEBERG_FORMAT_VERSION, "3");
    Properties tableConfig = new Properties();
    tableConfig.setProperty(IcebergFormatConfig.FORMAT_VERSION, "2");
    assertEquals(2, IcebergFormatConfig.of(tableConfig, conf).getFormatVersion());

    tableConfig.setProperty(IcebergFormatConfig.FORMAT_VERSION, "3");
    conf.set(IcebergTableManager.ICEBERG_FORMAT_VERSION, "2");
    assertEquals(3, IcebergFormatConfig.of(tableConfig, conf).getFormatVersion());
  }

  @Test
  void locationsMatchAcrossSchemeSpellingsOnly() {
    assertTrue(IcebergFormatConfig.sameLocation("file:/tmp/table", "/tmp/table"));
    assertTrue(IcebergFormatConfig.sameLocation("file:///tmp/table/", "file:/tmp/table"));
    assertTrue(IcebergFormatConfig.sameLocation("s3://bucket/a/table", "s3a://bucket/a/table"));
    assertFalse(IcebergFormatConfig.sameLocation("file:/tmp/other", "/tmp/table"));
    assertFalse(IcebergFormatConfig.sameLocation("s3://bucket/a/table", "s3a://other/a/table"));
    assertFalse(IcebergFormatConfig.sameLocation("gs://bucket/a/table", "s3a://bucket/a/table"));
  }
}
