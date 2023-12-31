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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import io.onetable.exception.NotSupportedException;
import io.onetable.model.storage.TableFormat;
import io.onetable.model.sync.SyncMode;
import io.onetable.spi.sync.TargetClient;

public class TestTableFormatClientFactory {

  @Test
  public void testTableClientFromNameForDELTA() {
    TargetClient tc =
        TableFormatClientFactory.getInstance().createTargetClientForName(TableFormat.DELTA);
    assertNotNull(tc);
    PerTableConfig perTableConfig =
        getPerTableConfig(Arrays.asList(TableFormat.DELTA), SyncMode.INCREMENTAL);
    Configuration conf = new Configuration();
    conf.setStrings("spark.master", "local");
    tc.init(perTableConfig, conf);
    assertNotNull(tc.getTableFormat().equalsIgnoreCase(TableFormat.DELTA));
  }

  @Test
  public void testTableClientFromNameForHUDI() {
    TargetClient tc =
        TableFormatClientFactory.getInstance().createTargetClientForName(TableFormat.HUDI);
    assertNotNull(tc);
    PerTableConfig perTableConfig =
        getPerTableConfig(Arrays.asList(TableFormat.HUDI), SyncMode.INCREMENTAL);
    Configuration conf = new Configuration();
    conf.setStrings("spark.master", "local");
    tc.init(perTableConfig, conf);
    assertNotNull(tc.getTableFormat().equalsIgnoreCase(TableFormat.HUDI));
  }

  @Test
  public void testTableClientFromNameForICEBERG() {
    TargetClient tc =
        TableFormatClientFactory.getInstance().createTargetClientForName(TableFormat.ICEBERG);
    assertNotNull(tc);
    PerTableConfig perTableConfig =
        getPerTableConfig(Arrays.asList(TableFormat.ICEBERG), SyncMode.INCREMENTAL);
    Configuration conf = new Configuration();
    conf.setStrings("spark.master", "local");
    tc.init(perTableConfig, conf);
    assertNotNull(tc.getTableFormat().equalsIgnoreCase(TableFormat.ICEBERG));
  }

  @Test
  public void testTableClientFromNameForUNKOWN() {
    try {
      TargetClient tc = TableFormatClientFactory.getInstance().createTargetClientForName("UNKOWN");
      fail("NotSupportedException expected and operation succeeded inappropriately.");
    } catch (NotSupportedException e) {
      // this is expected
    }
  }

  @Test
  public void testTableClientFromFormatType() {
    PerTableConfig perTableConfig =
        getPerTableConfig(Arrays.asList(TableFormat.DELTA), SyncMode.INCREMENTAL);
    Configuration conf = new Configuration();
    conf.setStrings("spark.master", "local");
    TargetClient tc =
        TableFormatClientFactory.getInstance()
            .createForFormat(TableFormat.DELTA, perTableConfig, conf);
    assertNotNull(tc.getTableFormat().equalsIgnoreCase(TableFormat.DELTA));
  }

  private PerTableConfig getPerTableConfig(List<String> targetTableFormats, SyncMode syncMode) {
    return PerTableConfigImpl.builder()
        .tableName(getTableName())
        .tableBasePath("/tmp/doesnt/matter")
        .targetTableFormats(targetTableFormats)
        .syncMode(syncMode)
        .build();
  }
}
