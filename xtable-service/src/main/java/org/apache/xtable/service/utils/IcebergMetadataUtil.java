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
 
package org.apache.xtable.service.utils;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.hadoop.HadoopTables;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class IcebergMetadataUtil {
  public Pair<String, String> getIcebergSchemaAndMetadataPath(
      String tableLocation, Configuration conf) {
    HadoopTables tables = new HadoopTables(conf);
    Table table = tables.load(tableLocation);
    TableOperations ops = ((BaseTable) table).operations();
    TableMetadata current = ops.current();
    return Pair.of(current.metadataFileLocation(), SchemaParser.toJson(current.schema()));
  }
}
