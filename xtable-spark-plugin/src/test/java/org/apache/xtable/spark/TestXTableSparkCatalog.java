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
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.xtable.model.storage.TableFormat;

/** Unit tests for XTableSparkCatalog format detection logic. */
public class TestXTableSparkCatalog {

  @Test
  public void testIsHudiFormat_DetectsHudiInInputFormat() {
    XTableSparkCatalog catalog = new XTableSparkCatalog();

    // Test with Hudi input format
    boolean result =
        catalog.isHudiFormat("org.apache.hudi.hadoop.HoodieParquetInputFormat", null, null);

    Assertions.assertTrue(result, "Should detect Hudi from input format");
  }

  @Test
  public void testIsHudiFormat_DetectsHudiInOutputFormat() {
    XTableSparkCatalog catalog = new XTableSparkCatalog();

    // Test with Hudi output format
    boolean result =
        catalog.isHudiFormat(null, "org.apache.hudi.hadoop.HoodieParquetOutputFormat", null);

    Assertions.assertTrue(result, "Should detect Hudi from output format");
  }

  @Test
  public void testIsHudiFormat_DetectsHudiInSerdeLib() {
    XTableSparkCatalog catalog = new XTableSparkCatalog();

    // Test with Hudi SerDe
    boolean result = catalog.isHudiFormat(null, null, "org.apache.hadoop.hive.hudi.HoodieSerde");

    Assertions.assertTrue(result, "Should detect Hudi from SerDe library");
  }

  @Test
  public void testIsHudiFormat_CaseInsensitive() {
    XTableSparkCatalog catalog = new XTableSparkCatalog();

    // Test case insensitivity
    boolean result = catalog.isHudiFormat("org.apache.HUDI.hadoop.InputFormat", null, null);

    Assertions.assertTrue(result, "Should be case insensitive");
  }

  @Test
  public void testIsHudiFormat_ReturnsFalseForNonHudi() {
    XTableSparkCatalog catalog = new XTableSparkCatalog();

    // Test with non-Hudi formats
    boolean result =
        catalog.isHudiFormat(
            "org.apache.hadoop.mapred.TextInputFormat",
            "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");

    Assertions.assertFalse(result, "Should return false for non-Hudi formats");
  }

  @Test
  public void testIsHudiFormat_HandlesNullValues() {
    XTableSparkCatalog catalog = new XTableSparkCatalog();

    // Test with all nulls
    boolean result = catalog.isHudiFormat(null, null, null);

    Assertions.assertFalse(result, "Should return false when all values are null");
  }

  @Test
  public void testIsIcebergFormat_DetectsIcebergInInputFormat() {
    XTableSparkCatalog catalog = new XTableSparkCatalog();

    // Test with Iceberg input format
    boolean result =
        catalog.isIcebergFormat("org.apache.iceberg.mr.hive.HiveIcebergInputFormat", null, null);

    Assertions.assertTrue(result, "Should detect Iceberg from input format");
  }

  @Test
  public void testIsIcebergFormat_DetectsIcebergInOutputFormat() {
    XTableSparkCatalog catalog = new XTableSparkCatalog();

    // Test with Iceberg output format
    boolean result =
        catalog.isIcebergFormat(null, "org.apache.iceberg.mr.hive.HiveIcebergOutputFormat", null);

    Assertions.assertTrue(result, "Should detect Iceberg from output format");
  }

  @Test
  public void testIsIcebergFormat_DetectsIcebergInSerdeLib() {
    XTableSparkCatalog catalog = new XTableSparkCatalog();

    // Test with Iceberg SerDe
    boolean result =
        catalog.isIcebergFormat(null, null, "org.apache.iceberg.mr.hive.HiveIcebergSerDe");

    Assertions.assertTrue(result, "Should detect Iceberg from SerDe library");
  }

  @Test
  public void testIsIcebergFormat_CaseInsensitive() {
    XTableSparkCatalog catalog = new XTableSparkCatalog();

    // Test case insensitivity
    boolean result = catalog.isIcebergFormat("org.apache.ICEBERG.mr.InputFormat", null, null);

    Assertions.assertTrue(result, "Should be case insensitive");
  }

  @Test
  public void testIsIcebergFormat_ReturnsFalseForNonIceberg() {
    XTableSparkCatalog catalog = new XTableSparkCatalog();

    // Test with non-Iceberg formats
    boolean result =
        catalog.isIcebergFormat(
            "org.apache.hadoop.mapred.TextInputFormat",
            "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");

    Assertions.assertFalse(result, "Should return false for non-Iceberg formats");
  }

  @Test
  public void testIsIcebergFormat_HandlesNullValues() {
    XTableSparkCatalog catalog = new XTableSparkCatalog();

    // Test with all nulls
    boolean result = catalog.isIcebergFormat(null, null, null);

    Assertions.assertFalse(result, "Should return false when all values are null");
  }

  @Test
  public void testDetermineTableFormatFromProperties_HudiViaProvider() {
    XTableSparkCatalog catalog = new XTableSparkCatalog();

    Map<String, String> properties = new HashMap<>();
    properties.put("provider", "hudi");

    String format = catalog.determineTableFormatFromProperties(properties);

    Assertions.assertEquals(TableFormat.HUDI, format, "Should detect Hudi from provider property");
  }

  @Test
  public void testDetermineTableFormatFromProperties_HudiViaSqlSourcesProvider() {
    XTableSparkCatalog catalog = new XTableSparkCatalog();

    Map<String, String> properties = new HashMap<>();
    properties.put("spark.sql.sources.provider", "hudi");

    String format = catalog.determineTableFormatFromProperties(properties);

    Assertions.assertEquals(
        TableFormat.HUDI, format, "Should detect Hudi from spark.sql.sources.provider");
  }

  @Test
  public void testDetermineTableFormatFromProperties_HudiViaTableType() {
    XTableSparkCatalog catalog = new XTableSparkCatalog();

    Map<String, String> properties = new HashMap<>();
    properties.put("table_type", "HUDI");

    String format = catalog.determineTableFormatFromProperties(properties);

    Assertions.assertEquals(TableFormat.HUDI, format, "Should detect Hudi from table_type");
  }

  @Test
  public void testDetermineTableFormatFromProperties_IcebergViaProvider() {
    XTableSparkCatalog catalog = new XTableSparkCatalog();

    Map<String, String> properties = new HashMap<>();
    properties.put("provider", "iceberg");

    String format = catalog.determineTableFormatFromProperties(properties);

    Assertions.assertEquals(
        TableFormat.ICEBERG, format, "Should detect Iceberg from provider property");
  }

  @Test
  public void testDetermineTableFormatFromProperties_IcebergViaTableType() {
    XTableSparkCatalog catalog = new XTableSparkCatalog();

    Map<String, String> properties = new HashMap<>();
    properties.put("table_type", "ICEBERG");

    String format = catalog.determineTableFormatFromProperties(properties);

    Assertions.assertEquals(TableFormat.ICEBERG, format, "Should detect Iceberg from table_type");
  }

  @Test
  public void testDetermineTableFormatFromProperties_DefaultsToIcebergWhenNull() {
    XTableSparkCatalog catalog = new XTableSparkCatalog();

    String format = catalog.determineTableFormatFromProperties(null);

    Assertions.assertEquals(
        TableFormat.ICEBERG, format, "Should default to Iceberg when properties are null");
  }

  @Test
  public void testDetermineTableFormatFromProperties_DefaultsToIcebergWhenEmpty() {
    XTableSparkCatalog catalog = new XTableSparkCatalog();

    Map<String, String> properties = new HashMap<>();

    String format = catalog.determineTableFormatFromProperties(properties);

    Assertions.assertEquals(
        TableFormat.ICEBERG, format, "Should default to Iceberg when properties are empty");
  }

  @Test
  public void testDetermineTableFormatFromProperties_DefaultsToIcebergForUnknownProvider() {
    XTableSparkCatalog catalog = new XTableSparkCatalog();

    Map<String, String> properties = new HashMap<>();
    properties.put("provider", "unknown");

    String format = catalog.determineTableFormatFromProperties(properties);

    Assertions.assertEquals(
        TableFormat.ICEBERG, format, "Should default to Iceberg for unknown providers");
  }

  @Test
  public void testDetermineTableFormatFromProperties_CaseInsensitiveProvider() {
    XTableSparkCatalog catalog = new XTableSparkCatalog();

    Map<String, String> properties = new HashMap<>();
    properties.put("provider", "HUDI");

    String format = catalog.determineTableFormatFromProperties(properties);

    Assertions.assertEquals(TableFormat.HUDI, format, "Should be case insensitive for provider");
  }

  @Test
  public void testDetermineTableFormatFromProperties_ProviderContainsHudi() {
    XTableSparkCatalog catalog = new XTableSparkCatalog();

    Map<String, String> properties = new HashMap<>();
    properties.put("provider", "org.apache.spark.sql.hudi");

    String format = catalog.determineTableFormatFromProperties(properties);

    Assertions.assertEquals(
        TableFormat.HUDI, format, "Should detect Hudi when provider contains 'hudi'");
  }

  @Test
  public void testDetermineTableFormatFromProperties_ProviderContainsIceberg() {
    XTableSparkCatalog catalog = new XTableSparkCatalog();

    Map<String, String> properties = new HashMap<>();
    properties.put("provider", "org.apache.iceberg.spark");

    String format = catalog.determineTableFormatFromProperties(properties);

    Assertions.assertEquals(
        TableFormat.ICEBERG, format, "Should detect Iceberg when provider contains 'iceberg'");
  }
}
