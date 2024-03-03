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

import static io.onetable.model.storage.TableFormat.DELTA;
import static io.onetable.model.storage.TableFormat.HUDI;
import static io.onetable.model.storage.TableFormat.ICEBERG;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.onetable.iceberg.IcebergCatalogConfig;
import io.onetable.utilities.RunSync.TableFormatClients;
import io.onetable.utilities.RunSync.TableFormatClients.ClientConfig;

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

  @Test
  public void testTableFormatClientConfigDefault() throws IOException {
    TableFormatClients clients = RunSync.loadTableFormatClientConfigs(null);
    Map<String, ClientConfig> tfClients = clients.getTableFormatsClients();
    Assertions.assertEquals(3, tfClients.size());
    Assertions.assertNotNull(tfClients.get(DELTA));
    Assertions.assertNotNull(tfClients.get(HUDI));
    Assertions.assertNotNull(tfClients.get(ICEBERG));

    Assertions.assertEquals(
        "io.onetable.hudi.HudiSourceClientProvider",
        tfClients.get(HUDI).getSourceClientProviderClass());
    Assertions.assertEquals(
        "io.onetable.iceberg.IcebergTargetClient",
        tfClients.get(ICEBERG).getTargetClientProviderClass());
    Assertions.assertEquals(
        "io.onetable.iceberg.IcebergSourceClientProvider",
        tfClients.get(ICEBERG).getSourceClientProviderClass());
  }

  @Test
  public void testTableFormatClientCustom() throws IOException {
    String customClients =
        "tableFormatsClients:\n"
            + "  HUDI:\n"
            + "    sourceClientProviderClass: foo\n"
            + "  DELTA:\n"
            + "    configuration:\n"
            + "      spark.master: local[4]\n"
            + "      foo: bar\n"
            + "  NEW_FORMAT:\n"
            + "    sourceClientProviderClass: bar\n";
    TableFormatClients clients = RunSync.loadTableFormatClientConfigs(customClients.getBytes());
    Map<String, ClientConfig> tfClients = clients.getTableFormatsClients();
    Assertions.assertEquals(4, tfClients.size());

    Assertions.assertNotNull(tfClients.get("NEW_FORMAT"));
    Assertions.assertEquals("bar", tfClients.get("NEW_FORMAT").getSourceClientProviderClass());

    Assertions.assertEquals("foo", tfClients.get(HUDI).getSourceClientProviderClass());

    Map<String, String> deltaClientConfigs = tfClients.get(DELTA).getConfiguration();
    Assertions.assertEquals(3, deltaClientConfigs.size());
    Assertions.assertEquals("local[4]", deltaClientConfigs.get("spark.master"));
    Assertions.assertEquals("bar", deltaClientConfigs.get("foo"));
  }

  @Test
  public void testIcebergCatalogConfig() throws IOException {
    String icebergConfig =
        "catalogImpl: io.onetable.CatalogImpl\n"
            + "catalogName: test\n"
            + "catalogOptions: \n"
            + "  option1: value1\n"
            + "  option2: value2";
    IcebergCatalogConfig catalogConfig = RunSync.loadIcebergCatalogConfig(icebergConfig.getBytes());
    Assertions.assertEquals("io.onetable.CatalogImpl", catalogConfig.getCatalogImpl());
    Assertions.assertEquals("test", catalogConfig.getCatalogName());
    Assertions.assertEquals(2, catalogConfig.getCatalogOptions().size());
    Assertions.assertEquals("value1", catalogConfig.getCatalogOptions().get("option1"));
    Assertions.assertEquals("value2", catalogConfig.getCatalogOptions().get("option2"));
  }
}
