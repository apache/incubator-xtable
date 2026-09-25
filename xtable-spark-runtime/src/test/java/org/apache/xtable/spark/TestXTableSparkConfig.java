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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import org.junit.jupiter.api.Test;

class TestXTableSparkConfig {

  private static final UnaryOperator<String> FAIL_RESOLVER =
      name -> {
        throw new AssertionError("nameResolver should not be called: " + name);
      };

  private static UnaryOperator<String> confOf(Map<String, String> map) {
    return map::get;
  }

  @Test
  void parsesPathBasedTableWithMultipleTargets() {
    Map<String, String> conf = new HashMap<>();
    conf.put("spark.xtable.tables", "orders");
    conf.put("spark.xtable.orders.basePath", "/warehouse/db/orders");
    conf.put("spark.xtable.orders.sourceFormat", "hudi");
    conf.put("spark.xtable.orders.targets", "iceberg, delta");

    List<TableSyncSpec> specs = XTableSparkConfig.parse(confOf(conf), FAIL_RESOLVER);

    assertEquals(1, specs.size());
    TableSyncSpec spec = specs.get(0);
    assertEquals("orders", spec.getKey());
    assertEquals("/warehouse/db/orders", spec.getBasePath());
    assertEquals("HUDI", spec.getSourceFormat());
    assertEquals(Arrays.asList("ICEBERG", "DELTA"), spec.getTargets());
    assertNull(spec.getDataPath());
    assertNull(spec.getNamespace());
  }

  @Test
  void parsesMultipleTablesWithOptionalFields() {
    Map<String, String> conf = new HashMap<>();
    conf.put("spark.xtable.tables", "orders , customers");
    conf.put("spark.xtable.orders.basePath", "/wh/orders");
    conf.put("spark.xtable.orders.sourceFormat", "HUDI");
    conf.put("spark.xtable.orders.targets", "ICEBERG");
    conf.put("spark.xtable.orders.dataPath", "/wh/orders/data");
    conf.put("spark.xtable.orders.namespace", "prod.sales");
    conf.put("spark.xtable.customers.basePath", "/wh/customers");
    conf.put("spark.xtable.customers.sourceFormat", "HUDI");
    conf.put("spark.xtable.customers.targets", "DELTA");

    List<TableSyncSpec> specs = XTableSparkConfig.parse(confOf(conf), FAIL_RESOLVER);

    assertEquals(2, specs.size());
    TableSyncSpec orders = specs.get(0);
    assertEquals("/wh/orders/data", orders.getDataPath());
    assertArrayEquals(new String[] {"prod", "sales"}, orders.getNamespace());
    assertEquals("customers", specs.get(1).getKey());
  }

  @Test
  void resolvesNameBasedTableViaResolver() {
    Map<String, String> conf = new HashMap<>();
    conf.put("spark.xtable.tables", "orders");
    conf.put("spark.xtable.orders.sourceTable", "db.orders");
    conf.put("spark.xtable.orders.sourceFormat", "HUDI");
    conf.put("spark.xtable.orders.targets", "ICEBERG");

    UnaryOperator<String> resolver =
        name -> "db.orders".equals(name) ? "s3://bucket/warehouse/orders" : null;

    List<TableSyncSpec> specs = XTableSparkConfig.parse(confOf(conf), resolver);

    assertEquals(1, specs.size());
    assertEquals("s3://bucket/warehouse/orders", specs.get(0).getBasePath());
  }

  @Test
  void basePathTakesPrecedenceOverNameSoResolverNotCalled() {
    Map<String, String> conf = new HashMap<>();
    conf.put("spark.xtable.tables", "orders");
    conf.put("spark.xtable.orders.basePath", "/wh/orders");
    conf.put("spark.xtable.orders.sourceTable", "db.orders");
    conf.put("spark.xtable.orders.sourceFormat", "HUDI");
    conf.put("spark.xtable.orders.targets", "ICEBERG");

    List<TableSyncSpec> specs = XTableSparkConfig.parse(confOf(conf), FAIL_RESOLVER);
    assertEquals("/wh/orders", specs.get(0).getBasePath());
  }

  @Test
  void emptyOrMissingTablesYieldsEmptyList() {
    assertTrue(XTableSparkConfig.parse(confOf(new HashMap<>()), FAIL_RESOLVER).isEmpty());
    Map<String, String> blank = new HashMap<>();
    blank.put("spark.xtable.tables", "  ");
    assertTrue(XTableSparkConfig.parse(confOf(blank), FAIL_RESOLVER).isEmpty());
  }

  @Test
  void missingBasePathAndSourceTableThrows() {
    Map<String, String> conf = new HashMap<>();
    conf.put("spark.xtable.tables", "orders");
    conf.put("spark.xtable.orders.sourceFormat", "HUDI");
    conf.put("spark.xtable.orders.targets", "ICEBERG");
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> XTableSparkConfig.parse(confOf(conf), FAIL_RESOLVER));
    assertTrue(e.getMessage().contains("basePath"));
  }

  @Test
  void missingSourceFormatThrows() {
    Map<String, String> conf = new HashMap<>();
    conf.put("spark.xtable.tables", "orders");
    conf.put("spark.xtable.orders.basePath", "/wh/orders");
    conf.put("spark.xtable.orders.targets", "ICEBERG");
    assertThrows(
        IllegalArgumentException.class, () -> XTableSparkConfig.parse(confOf(conf), FAIL_RESOLVER));
  }

  @Test
  void missingTargetsThrows() {
    Map<String, String> conf = new HashMap<>();
    conf.put("spark.xtable.tables", "orders");
    conf.put("spark.xtable.orders.basePath", "/wh/orders");
    conf.put("spark.xtable.orders.sourceFormat", "HUDI");
    assertThrows(
        IllegalArgumentException.class, () -> XTableSparkConfig.parse(confOf(conf), FAIL_RESOLVER));
  }
}
