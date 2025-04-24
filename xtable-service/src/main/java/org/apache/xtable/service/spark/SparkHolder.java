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
 
package org.apache.xtable.service.spark;

import java.io.InputStream;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import org.apache.xtable.service.ConversionServiceUtil;

import io.quarkus.runtime.ShutdownEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;

@ApplicationScoped
public class SparkHolder {

  private volatile SparkSession spark;
  private volatile JavaSparkContext jsc;
  private static final String HADOOP_DEFAULTS_PATH = "xtable-hadoop-defaults.xml";

  public SparkSession spark() {
    if (spark == null) {
      synchronized (this) {
        if (spark == null) {
          spark =
              SparkSession.builder()
                  .config(ConversionServiceUtil.getSparkConf())
                  .appName("xtable-conversion-service")
                  .getOrCreate();
          jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
          InputStream resourceStream =
              SparkHolder.class.getClassLoader().getResourceAsStream(HADOOP_DEFAULTS_PATH);
          if (resourceStream != null) {
            spark.sparkContext().hadoopConfiguration().addResource(resourceStream);
          } else {
            throw new RuntimeException("Failed to load resource xtable-hadoop-defaults.xml");
          }
        }
      }
    }
    return spark;
  }

  public JavaSparkContext jsc() {
    // ensure spark() has run
    spark();
    return jsc;
  }

  // cleanly stop Spark when Quarkus shuts down
  void onShutdown(@Observes ShutdownEvent ev) {
    if (spark != null) {
      spark.stop();
    }
  }
}
