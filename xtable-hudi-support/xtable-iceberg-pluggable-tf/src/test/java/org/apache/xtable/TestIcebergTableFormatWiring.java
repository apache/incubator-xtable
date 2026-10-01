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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.Properties;

import org.junit.jupiter.api.Test;

import org.apache.xtable.conversion.TargetTable;
import org.apache.xtable.metadata.IcebergMetadataFactory;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.timeline.IcebergTimelineFactory;

class TestIcebergTableFormatWiring {

  private static IcebergTableFormat tableFormat() {
    IcebergTableFormat tableFormat = new IcebergTableFormat();
    tableFormat.init(new Properties());
    return tableFormat;
  }

  @Test
  void nameMatchesTheValueWrittenToHoodieProperties() {
    assertEquals(TableFormat.ICEBERG, tableFormat().getName());
  }

  @Test
  void neverExpiresSnapshotsByAge() {
    // The reconstructed timeline treats a completed instant without a snapshot as inflight, so
    // snapshots may only go away when Hudi archives their instants.
    assertEquals(
        TargetTable.NO_METADATA_EXPIRY,
        IcebergTableFormat.targetTable("table", "/base").getMetadataRetention());
  }

  @Test
  void suppliesTheIcebergTimelineAndMetadataFactories() {
    assertInstanceOf(IcebergTimelineFactory.class, tableFormat().getTimelineFactory());
    assertInstanceOf(IcebergMetadataFactory.class, tableFormat().getMetadataFactory());
  }
}
