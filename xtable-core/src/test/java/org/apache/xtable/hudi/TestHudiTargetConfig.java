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
 
package org.apache.xtable.hudi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Properties;

import org.junit.jupiter.api.Test;

import org.apache.hudi.common.table.HoodieTableVersion;

public class TestHudiTargetConfig {

  @Test
  void defaultsToTableVersionNine() {
    assertEquals(
        HoodieTableVersion.NINE,
        HudiTargetConfig.fromProperties(new Properties()).getTableVersion());
    assertEquals(HoodieTableVersion.NINE, HudiTargetConfig.fromProperties(null).getTableVersion());
  }

  @Test
  void honoursConfiguredVersion() {
    Properties sixProps = new Properties();
    sixProps.setProperty(HudiTargetConfig.HUDI_TABLE_VERSION, "6");
    assertEquals(
        HoodieTableVersion.SIX, HudiTargetConfig.fromProperties(sixProps).getTableVersion());

    Properties nineProps = new Properties();
    nineProps.setProperty(HudiTargetConfig.HUDI_TABLE_VERSION, "9");
    assertEquals(
        HoodieTableVersion.NINE, HudiTargetConfig.fromProperties(nineProps).getTableVersion());
  }

  @Test
  void rejectsUnsupportedVersion() {
    Properties props = new Properties();
    props.setProperty(HudiTargetConfig.HUDI_TABLE_VERSION, "8");
    assertThrows(IllegalArgumentException.class, () -> HudiTargetConfig.fromProperties(props));
  }
}
