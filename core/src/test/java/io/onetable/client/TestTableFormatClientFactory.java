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
 
package io.onetable.client;

import static io.onetable.GenericTable.getTableName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import io.onetable.exception.NotSupportedException;
import io.onetable.model.storage.TableFormat;
import io.onetable.spi.sync.TargetClient;

public class TestTableFormatClientFactory {

  @Test
  public void testTableClientFromNameForDELTA() {
    TargetClient tc =
        TableFormatClientFactory.getInstance().createTargetClientForName(TableFormat.DELTA);
    assertNotNull(tc);
    TargetTable targetTable = getPerTableConfig(TableFormat.DELTA);
    Configuration conf = new Configuration();
    conf.setStrings("spark.master", "local");
    tc.init(targetTable, conf);
    assertEquals(tc.getTableFormat(), TableFormat.DELTA);
  }

  @Test
  public void testTableClientFromNameForHUDI() {
    TargetClient tc =
        TableFormatClientFactory.getInstance().createTargetClientForName(TableFormat.HUDI);
    assertNotNull(tc);
    TargetTable targetTable = getPerTableConfig(TableFormat.HUDI);
    Configuration conf = new Configuration();
    conf.setStrings("spark.master", "local");
    tc.init(targetTable, conf);
    assertEquals(tc.getTableFormat(), TableFormat.HUDI);
  }

  @Test
  public void testTableClientFromNameForICEBERG() {
    TargetClient tc =
        TableFormatClientFactory.getInstance().createTargetClientForName(TableFormat.ICEBERG);
    assertNotNull(tc);
    TargetTable perTableConfig = getPerTableConfig(TableFormat.ICEBERG);
    Configuration conf = new Configuration();
    conf.setStrings("spark.master", "local");
    tc.init(perTableConfig, conf);
    assertEquals(tc.getTableFormat(), TableFormat.ICEBERG);
  }

  @Test
  public void testTableClientFromNameForUNKOWN() {
    NotSupportedException thrown =
        assertThrows(
            NotSupportedException.class,
            () -> TableFormatClientFactory.getInstance().createTargetClientForName("UNKNOWN"),
            "NotSupportedException expected and operation succeeded inappropriately.");
    assertTrue(thrown.getMessage().contains("UNKNOWN"));
  }

  @Test
  public void testTableClientFromFormatType() {
    TargetTable perTableConfig = getPerTableConfig(TableFormat.DELTA);
    Configuration conf = new Configuration();
    conf.setStrings("spark.master", "local");
    TargetClient tc = TableFormatClientFactory.getInstance().createForFormat(perTableConfig, conf);
    assertEquals(tc.getTableFormat(), TableFormat.DELTA);
  }

  private TargetTable getPerTableConfig(String tableFormat) {
    return TargetTable.builder()
        .name(getTableName())
        .basePath("/tmp/doesnt/matter")
        .formatName(tableFormat)
        .build();
  }
}
