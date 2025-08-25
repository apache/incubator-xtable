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
 
package org.apache.xtable.parquet;

import java.nio.charset.StandardCharsets;

import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;

public class ParquetStatsConverterUtil {
  public static Object convertStatBinaryTypeToLogicalType(
      ColumnChunkMetaData columnMetaData, boolean isMin) {
    Object returnedObj = null;
    PrimitiveType primitiveType = columnMetaData.getPrimitiveType();
    switch (primitiveType.getPrimitiveTypeName()) {
      case BINARY:// TODO check if other primitiveType' needs to be handled as well
        if (primitiveType.getLogicalTypeAnnotation() != null) {
          if (columnMetaData
              .getPrimitiveType()
              .getLogicalTypeAnnotation()
              .toString()
              .equals("STRING")) {
            returnedObj =
                new String(
                    (isMin
                            ? (Binary) columnMetaData.getStatistics().genericGetMin()
                            : (Binary) columnMetaData.getStatistics().genericGetMax())
                        .getBytes(),
                    StandardCharsets.UTF_8);
          } else {
            returnedObj =
                isMin
                    ? columnMetaData.getStatistics().genericGetMin()
                    : columnMetaData.getStatistics().genericGetMax();
          }
        } else {
          returnedObj =
              isMin
                  ? columnMetaData.getStatistics().genericGetMin()
                  : columnMetaData.getStatistics().genericGetMax();
        }
        break;
      default:
        returnedObj =
                isMin
                        ? columnMetaData.getStatistics().genericGetMin()
                        : columnMetaData.getStatistics().genericGetMax();
        // TODO JSON and DECIMAL... of BINARY primitiveType
    }
    return returnedObj;
  }
}
