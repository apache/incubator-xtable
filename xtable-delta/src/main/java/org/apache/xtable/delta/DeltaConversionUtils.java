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
 
package org.apache.xtable.delta;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.SparkSession;

/** A utility class for Delta conversion. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DeltaConversionUtils {

  static SparkSession buildSparkSession(Configuration conf) {
    SparkConf sparkConf =
        new SparkConf()
            .setAppName("xtable")
            .set("spark.serializer", KryoSerializer.class.getName())
            .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .set(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .set("spark.databricks.delta.constraints.allowUnenforcedNotNull.enabled", "true");
    SparkSession.Builder builder = SparkSession.builder().config(sparkConf);
    conf.forEach(
        entry ->
            builder.config(
                entry.getKey().startsWith("spark")
                    ? entry.getKey()
                    : "spark.hadoop." + entry.getKey(),
                entry.getValue()));
    return builder.getOrCreate();
  }
}
