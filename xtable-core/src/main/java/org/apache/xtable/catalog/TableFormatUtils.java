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

import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;
import static org.apache.xtable.catalog.Constants.PROP_SPARK_SQL_SOURCES_PROVIDER;

import java.util.Map;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.iceberg.TableProperties;

import com.google.common.base.Strings;

import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.model.storage.TableFormat;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TableFormatUtils {

  public static String getTableDataLocation(
      String tableFormat, String tableLocation, Map<String, String> properties) {
    switch (tableFormat) {
      case TableFormat.ICEBERG:
        return getIcebergDataLocation(tableLocation, properties);
      case TableFormat.DELTA:
      case TableFormat.HUDI:
        return tableLocation;
      default:
        throw new NotSupportedException("Unsupported table format: " + tableFormat);
    }
  }

  /** Get iceberg table data files location */
  private static String getIcebergDataLocation(
      String tableLocation, Map<String, String> properties) {
    String dataLocation = properties.get(TableProperties.WRITE_DATA_LOCATION);
    if (dataLocation == null) {
      dataLocation = properties.get(TableProperties.WRITE_FOLDER_STORAGE_LOCATION);
      if (dataLocation == null) {
        dataLocation = properties.get(TableProperties.OBJECT_STORE_PATH);
        if (dataLocation == null) {
          dataLocation = String.format("%s/data", tableLocation);
        }
      }
    }
    return dataLocation;
  }

  /**
   * Get table format from given table properties
   *
   * @param properties catalog table properties
   * @return table format name
   *     <li>In case of ICEBERG, table_type param will give the table format
   *     <li>In case of DELTA, table_type or spark.sql.sources.provider param will give the table
   *         format
   *     <li>In case of HUDI, spark.sql.sources.provider param will give the table format
   */
  public static String getTableFormat(Map<String, String> properties) {
    String tableFormat = properties.get(TABLE_TYPE_PROP);
    if (Strings.isNullOrEmpty(tableFormat)) {
      tableFormat = properties.get(PROP_SPARK_SQL_SOURCES_PROVIDER);
    }
    return tableFormat;
  }
}
