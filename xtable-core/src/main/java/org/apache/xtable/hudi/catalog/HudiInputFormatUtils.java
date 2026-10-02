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
 
package org.apache.xtable.hudi.catalog;

import org.apache.hudi.common.model.HoodieFileFormat;

import org.apache.xtable.exception.NotSupportedException;

public class HudiInputFormatUtils {

  /** Input Format, that provides a real-time view of data in a Hoodie table. */
  private static final String HUDI_PARQUET_REALTIME_INPUT_FORMAT_CLASS =
      "org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat";

  /**
   * HoodieInputFormat which understands the Hoodie File Structure and filters files based on the
   * Hoodie Mode.
   */
  private static final String HUDI_PARQUET_INPUT_FORMAT_CLASS =
      "org.apache.hudi.hadoop.HoodieParquetInputFormat";

  /** HoodieRealtimeInputFormat for HUDI datasets which store data in HFile base file format. */
  private static final String HUDI_HFILE_REALTIME_INPUT_FORMAT_CLASS =
      "org.apache.hudi.hadoop.realtime.HoodieHFileRealtimeInputFormat";

  /** HoodieInputFormat for HUDI datasets which store data in HFile base file format. */
  private static final String HUDI_HFILE_INPUT_FORMAT_CLASS =
      "org.apache.hudi.hadoop.HoodieHFileInputFormat";

  /** A MapReduce/ Hive input format for ORC files. */
  private static final String ORC_INPUT_FORMAT_CLASS =
      "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";

  /** A Hive OutputFormat for ORC files. */
  private static final String ORC_OUTPUT_FORMAT_CLASS =
      "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat";

  /** A Parquet OutputFormat for Hive (with the deprecated package mapred) */
  private static final String MAPRED_PARQUET_OUTPUT_FORMAT_CLASS =
      "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";

  /** A ParquetHiveSerDe for Hive (with the deprecated package mapred) */
  private static final String PARQUET_SERDE_CLASS =
      "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";

  /**
   * A serde class for ORC. It transparently passes the object to/ from the ORC file reader/ writer.
   */
  private static final String ORC_SERDE_CLASS = "org.apache.hadoop.hive.ql.io.orc.OrcSerde";

  public static String getInputFormatClassName(HoodieFileFormat baseFileFormat, boolean realtime) {
    switch (baseFileFormat) {
      case PARQUET:
        if (realtime) {
          return HUDI_PARQUET_REALTIME_INPUT_FORMAT_CLASS;
        } else {
          return HUDI_PARQUET_INPUT_FORMAT_CLASS;
        }
      case HFILE:
        if (realtime) {
          return HUDI_HFILE_REALTIME_INPUT_FORMAT_CLASS;
        } else {
          return HUDI_HFILE_INPUT_FORMAT_CLASS;
        }
      case ORC:
        return ORC_INPUT_FORMAT_CLASS;
      default:
        throw new NotSupportedException(
            "Hudi InputFormat not implemented for base file format " + baseFileFormat);
    }
  }

  public static String getOutputFormatClassName(HoodieFileFormat baseFileFormat) {
    switch (baseFileFormat) {
      case PARQUET:
      case HFILE:
        return MAPRED_PARQUET_OUTPUT_FORMAT_CLASS;
      case ORC:
        return ORC_OUTPUT_FORMAT_CLASS;
      default:
        throw new NotSupportedException("No OutputFormat for base file format " + baseFileFormat);
    }
  }

  public static String getSerDeClassName(HoodieFileFormat baseFileFormat) {
    switch (baseFileFormat) {
      case PARQUET:
      case HFILE:
        return PARQUET_SERDE_CLASS;
      case ORC:
        return ORC_SERDE_CLASS;
      default:
        throw new NotSupportedException("No SerDe for base file format " + baseFileFormat);
    }
  }
}
