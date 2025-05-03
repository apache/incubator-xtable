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

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.types.StructType;

@ApplicationScoped
public class DeltaMetadataUtil {
    public Pair<String, String> getDeltaSchemaAndMetadataPath(String basePath, SparkSession sparkSession) {
        DeltaLog deltaLog = DeltaLog.forTable(sparkSession, basePath);
        Snapshot snapshot = deltaLog.snapshot();
        StructType schema = snapshot.metadata().schema();
        String metadataPath = snapshot.path().toString();
        String schemaStr =  schema.json();
        return Pair.of(metadataPath, schemaStr);
    }
}
