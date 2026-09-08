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

/** Spark configuration keys and Iceberg table properties understood by the plugin. */
public final class HudiIcebergConf {
  private HudiIcebergConf() {}

  /** Global kill switch. */
  public static final String ENABLED = "spark.hudi.iceberg.enabled";
  /** Set by the driver plugin so {@code SET spark.hudi.iceberg.version} answers what is loaded. */
  public static final String VERSION = "spark.hudi.iceberg.version";
  /** New Iceberg tables created through a wrapped catalog become Hudi-managed. Default true. */
  public static final String MANAGE_NEW_TABLES = "spark.hudi.iceberg.manage-new-tables";
  /** Prefix of Spark confs forwarded to every Hudi write as {@code hoodie.<rest>}. */
  public static final String WRITE_CONF_PREFIX = "spark.hudi.iceberg.write.";
  /**
   * What to do with a write to a managed table that the plugin could not route through Hudi: {@code
   * reject} (default) fails the statement with the reason, {@code allow} lets Iceberg's own writer
   * commit it, leaving Hudi unaware of the resulting files.
   */
  public static final String FOREIGN_WRITES = "spark.hudi.iceberg.foreign-writes";

  public static final String FOREIGN_WRITES_REJECT = "reject";
  public static final String FOREIGN_WRITES_ALLOW = "allow";

  /** Per-catalog opt out: {@code spark.sql.catalog.<name>.hudi.enabled=false}. */
  public static final String CATALOG_OPTION_ENABLED = "hudi.enabled";

  public static final String CATALOG_OPTION_MANAGE_NEW_TABLES = "hudi.manage-new-tables";

  /** Iceberg table property marking a table whose writes go through Hudi. */
  public static final String TABLE_PROP_MANAGED = "hudi.managed";
  /** Optional Iceberg table property forcing the Hudi table type: {@code cow} or {@code mor}. */
  public static final String TABLE_PROP_TABLE_TYPE = "hudi.table-type";
  /** Optional Iceberg table property naming the Hudi ordering (pre-combine) field. */
  public static final String TABLE_PROP_ORDERING_FIELD = "hudi.ordering-field";
  /** Prefix of Iceberg table properties forwarded to every Hudi write as {@code hoodie.<rest>}. */
  public static final String TABLE_PROP_WRITE_PREFIX = "hudi.write.";

  public static final String SPARK_CATALOG_CLASS = "org.apache.iceberg.spark.SparkCatalog";
  public static final String KRYO_SERIALIZER = "org.apache.spark.serializer.KryoSerializer";
}
