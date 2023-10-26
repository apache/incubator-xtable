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
 
package io.onetable.client;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.hadoop.conf.Configuration;

import io.onetable.delta.DeltaClient;
import io.onetable.exception.NotSupportedException;
import io.onetable.hudi.HudiTargetClient;
import io.onetable.iceberg.IcebergClient;
import io.onetable.model.storage.TableFormat;
import io.onetable.spi.sync.TableFormatSync;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TableFormatClientFactory {
  public static TableFormatSync createForFormat(
      TableFormat tableFormat, PerTableConfig perTableConfig, Configuration configuration) {
    switch (tableFormat) {
      case ICEBERG:
        return TableFormatSync.of(new IcebergClient(perTableConfig, configuration));
      case DELTA:
        return TableFormatSync.of(new DeltaClient(perTableConfig, configuration));
      case HUDI:
        return TableFormatSync.of(new HudiTargetClient(perTableConfig, configuration));
      default:
        throw new NotSupportedException("Target format is not yet supported: " + tableFormat);
    }
  }
}
