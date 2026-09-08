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

import java.util.Arrays;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

/**
 * Turns every {@code spark.sql.catalog.<name>=org.apache.iceberg.spark.SparkCatalog} entry into the
 * Hudi-managed catalog unless that catalog opted out. Applied twice: on the SparkConf by the driver
 * plugin (covers spark-submit), and on the session conf when the session extension is built (covers
 * {@code SparkSession.builder().config(...)}, whose non-static options are re-applied on top of the
 * SparkContext conf and would otherwise undo the first rewrite).
 */
public final class HudiCatalogRewrite {
  private static final Logger LOG = LoggerFactory.getLogger(HudiCatalogRewrite.class);
  private static final String CATALOG_PREFIX = "spark.sql.catalog.";

  private HudiCatalogRewrite() {}

  public static void rewrite(SparkConf conf) {
    rewrite(
        Arrays.asList(conf.getAll()),
        key -> conf.get(key, null),
        (key, value) -> conf.set(key, value));
  }

  public static void rewrite(SparkSession session) {
    if (!session.conf().get(HudiIcebergConf.ENABLED, "true").equalsIgnoreCase("true")) {
      return;
    }
    rewrite(
        scala.collection.JavaConverters.mapAsJavaMapConverter(session.conf().getAll())
            .asJava()
            .entrySet()
            .stream()
            .map(e -> new Tuple2<>(e.getKey(), e.getValue()))
            .collect(java.util.stream.Collectors.toList()),
        key -> session.conf().get(key, null),
        (key, value) -> session.conf().set(key, value));
  }

  private static void rewrite(
      Iterable<Tuple2<String, String>> entries,
      Function<String, String> getter,
      BiConsumer<String, String> setter) {
    for (Tuple2<String, String> entry : entries) {
      String key = entry._1();
      if (!key.startsWith(CATALOG_PREFIX) || key.substring(CATALOG_PREFIX.length()).contains(".")) {
        continue;
      }
      if (!HudiIcebergConf.SPARK_CATALOG_CLASS.equals(entry._2())) {
        continue;
      }
      String optOut = getter.apply(key + "." + HudiIcebergConf.CATALOG_OPTION_ENABLED);
      if (optOut != null && optOut.equalsIgnoreCase("false")) {
        LOG.info("Leaving catalog {} on the plain Iceberg SparkCatalog (opted out)", key);
        continue;
      }
      setter.accept(key, HudiSparkCatalog.class.getName());
      LOG.info("Setting {}={}", key, HudiSparkCatalog.class.getName());
    }
  }
}
