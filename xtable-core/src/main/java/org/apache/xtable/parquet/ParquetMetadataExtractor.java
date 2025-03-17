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
import org.apache.hadoop.fs.*;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.hadoop.fs.Path;
public class ParquetMetadataExtractor {

  private static final ParquetMetadataExtractor INSTANCE = new ParquetMetadataExtractor();
  public static ParquetMetadataExtractor getInstance() {
    return INSTANCE;
  }

  public static MessageType getSchema(ParquetMetadata footer) {
    MessageType schema = footer.getFileMetaData().getSchema();
    return schema;
  }

  public static ParquetMetadata readParquetMetadata(Configuration conf, Path path) {
    ParquetMetadata footer =null;
        //ParquetFileReader.readFooter(conf, path, ParquetMetadataConverter.NO_FILTER);
    return footer;
  }
}
