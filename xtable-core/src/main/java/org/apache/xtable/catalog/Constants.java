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
 
package org.apache.xtable.catalog;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Constants {

  /**
   * This property should be used to specify the data source provider that Spark uses to read from
   * or write to the data source. For ex: when working with a Delta table in Glue / HMS, this
   * property typically points to "delta", and "hudi" in case of a Hudi table
   */
  public static final String PROP_SPARK_SQL_SOURCES_PROVIDER = "spark.sql.sources.provider";

  /** This property should be used to specify the location of data in a storage system */
  public static final String PROP_PATH = "path";

  /**
   * This property should be used to specify the serialization format in Hive SerDe properties, and
   * it helps Hive understand how to read/write data for the table and is typically used in
   * conjunction with the InputFormat and OutputFormat properties
   */
  public static final String PROP_SERIALIZATION_FORMAT = "serialization.format";

  /**
   * This property should be used to specify whether a table being synced to a catalog is an
   * external table or not
   */
  public static final String PROP_EXTERNAL = "EXTERNAL";
}
