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
 
package org.apache.xtable.hudi.extensions;

import lombok.extern.slf4j.Slf4j;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hudi.callback.HoodieClientInitCallback;
import org.apache.hudi.client.BaseHoodieClient;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;

import com.google.common.annotations.VisibleForTesting;

import org.apache.xtable.hudi.idtracking.IdTracker;

/**
 * An implementation of {@link HoodieClientInitCallback} that adds field ID metadata to the schema.
 * When used with {@link HoodieAvroWriteSupportWithFieldIds}, ID values will be set on the fields in
 * the parquet file making them compatible with Iceberg readers that do not support the default
 * field id mapping.
 */
@Slf4j
public class AddFieldIdsClientInitCallback implements HoodieClientInitCallback {
  private final IdTracker idTracker;

  public AddFieldIdsClientInitCallback() {
    this(IdTracker.getInstance());
  }

  @VisibleForTesting
  AddFieldIdsClientInitCallback(IdTracker idTracker) {
    this.idTracker = idTracker;
  }

  @Override
  public void call(BaseHoodieClient hoodieClient) {
    HoodieWriteConfig config = hoodieClient.getConfig();
    if (config.getSchema() != null || config.getWriteSchema() != null) {
      try {
        Option<Schema> currentSchema = Option.empty();
        try {
          Configuration hadoopConfiguration = hoodieClient.getEngineContext().getHadoopConf().get();
          String tableBasePath = config.getBasePath();
          FileSystem fs = FSUtils.getFs(tableBasePath, hadoopConfiguration);
          if (FSUtils.isTableExists(config.getBasePath(), fs)) {
            HoodieTableMetaClient metaClient =
                HoodieTableMetaClient.builder()
                    .setConf(hadoopConfiguration)
                    .setBasePath(tableBasePath)
                    .build();
            currentSchema =
                new TableSchemaResolver(metaClient).getTableAvroSchemaFromLatestCommit(true);
          }
        } catch (Exception ex) {
          log.warn("Unable to fetch current schema for fieldIds", ex);
        }
        if (config.getSchema() != null) {
          Schema schema = new Schema.Parser().parse(config.getSchema());
          if (schema.getType() != Schema.Type.NULL) {
            Schema newSchema =
                idTracker.addIdTracking(schema, currentSchema, config.populateMetaFields());
            config.setSchema(newSchema.toString());
          }
        }
        if (config.getProps().containsKey(HoodieWriteConfig.WRITE_SCHEMA_OVERRIDE.key())) {
          Schema writeSchema = new Schema.Parser().parse(config.getWriteSchema());
          if (writeSchema.getType() != Schema.Type.NULL) {
            Schema newWriteSchema =
                idTracker.addIdTracking(writeSchema, currentSchema, config.populateMetaFields());
            config
                .getProps()
                .setProperty(
                    HoodieWriteConfig.WRITE_SCHEMA_OVERRIDE.key(), newWriteSchema.toString());
          }
        }
      } catch (Exception ex) {
        throw new HoodieException(
            "Unable to initialize fieldIds schema "
                + config.getSchema()
                + " write "
                + config.getWriteSchema(),
            ex);
      }
    }
  }
}
