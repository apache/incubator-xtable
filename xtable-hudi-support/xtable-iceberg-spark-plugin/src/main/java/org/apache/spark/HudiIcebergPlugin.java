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
 
package org.apache.spark;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.ExecutorPlugin;
import org.apache.spark.api.plugin.PluginContext;
import org.apache.spark.api.plugin.SparkPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.xtable.spark.HudiCatalogRewrite;
import org.apache.xtable.spark.HudiIcebergConf;
import org.apache.xtable.spark.HudiIcebergExtensions;

/**
 * The single entry point: {@code spark.plugins=org.apache.spark.HudiIcebergPlugin}. The driver
 * plugin runs inside the SparkContext constructor, before any SparkSession exists, and rewrites the
 * live SparkConf so that the Hudi session extension is registered and every Iceberg {@code
 * SparkCatalog} becomes a {@link HudiSparkCatalog}. Lives in {@code org.apache.spark} because
 * {@code SparkContext.conf} is package-private and {@code getConf} returns a copy.
 */
public class HudiIcebergPlugin implements SparkPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(HudiIcebergPlugin.class);
  public static final String VERSION = "0.5.0-SNAPSHOT";

  @Override
  public DriverPlugin driverPlugin() {
    return new HudiIcebergDriverPlugin();
  }

  @Override
  public ExecutorPlugin executorPlugin() {
    return null;
  }

  static class HudiIcebergDriverPlugin implements DriverPlugin {
    @Override
    public Map<String, String> init(SparkContext sc, PluginContext pluginContext) {
      SparkConf conf = sc.conf();
      conf.set(HudiIcebergConf.VERSION, VERSION);
      if (!conf.getBoolean(HudiIcebergConf.ENABLED, true)) {
        LOG.info("Hudi Iceberg plugin is disabled ({}=false)", HudiIcebergConf.ENABLED);
        return Collections.emptyMap();
      }
      String serializer = conf.get("spark.serializer", "");
      if (!HudiIcebergConf.KRYO_SERIALIZER.equals(serializer)) {
        LOG.warn(
            "Hudi Iceberg plugin is disabled because spark.serializer is '{}'. Hudi writes need "
                + "spark.serializer={} and it cannot be changed after the SparkContext starts.",
            serializer,
            HudiIcebergConf.KRYO_SERIALIZER);
        conf.set(HudiIcebergConf.ENABLED, "false");
        return Collections.emptyMap();
      }
      registerExtension(conf);
      HudiCatalogRewrite.rewrite(conf);
      return Collections.emptyMap();
    }

    static void registerExtension(SparkConf conf) {
      String key = "spark.sql.extensions";
      String extension = HudiIcebergExtensions.class.getName();
      String current = conf.get(key, "");
      List<String> extensions = new ArrayList<>();
      for (String name : current.split(",")) {
        if (!name.trim().isEmpty()) {
          extensions.add(name.trim());
        }
      }
      if (!extensions.contains(extension)) {
        extensions.add(extension);
        conf.set(key, String.join(",", extensions));
        LOG.info("Setting {}={}", key, conf.get(key));
      }
    }
  }
}
