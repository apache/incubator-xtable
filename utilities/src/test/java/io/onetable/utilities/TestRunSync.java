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
 
package io.onetable.utilities;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestRunSync {

  /** Tests that the default hadoop configs are loaded. */
  @Test
  public void testLoadDefaultHadoopConfig() {
    Configuration conf;
    conf = new Configuration();
    String value = conf.get("fs.file.impl");
    Assertions.assertNull(value);

    conf = RunSync.loadHadoopConf(null);
    value = conf.get("fs.file.impl");
    Assertions.assertEquals("org.apache.hadoop.fs.LocalFileSystem", value);
  }

  /** Tests that the custom hadoop configs are loaded and can override defaults. */
  @Test
  public void testLoadCustomHadoopConfig() {
    Configuration conf;
    conf = new Configuration();
    String value = conf.get("fs.azure.account.oauth2.client.endpoint");
    Assertions.assertNull(value);

    // build a custom hadoop config
    String customXmlConfig =
        "<configuration>"
            + "  <property>"
            + "    <name>fs.file.impl</name>"
            + "    <value>override_default_value</value>"
            + "  </property>"
            + "  <property>"
            + "    <name>fs.azure.account.oauth2.client.endpoint</name>"
            + "    <value>https://login.microsoftonline.com/</value>"
            + "  </property>"
            + "</configuration>";

    conf = RunSync.loadHadoopConf(customXmlConfig.getBytes());
    value = conf.get("fs.file.impl");
    Assertions.assertEquals("override_default_value", value);
    value = conf.get("fs.azure.account.oauth2.client.endpoint");
    Assertions.assertEquals("https://login.microsoftonline.com/", value);
  }
}
