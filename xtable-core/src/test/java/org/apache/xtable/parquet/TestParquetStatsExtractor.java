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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.apache.parquet.schema.*;
import org.junit.jupiter.api.Assertions;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.GroupType;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.format.Statistics;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.xtable.model.storage.InternalDataFile;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.xtable.model.storage.FileFormat;
import static org.apache.parquet.column.Encoding.BIT_PACKED;
import static org.apache.parquet.column.Encoding.PLAIN;

import java.io.File;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import java.io.IOException;

import org.apache.parquet.schema.MessageTypeParser;


public class TestParquetStatsExtractor {

    public static Path createParquetFile(File file) throws IOException {
        Path path = new Path(file.toURI());
        Configuration configuration = new Configuration();

        MessageType schema = MessageTypeParser.parseMessageType("message m { required group a {required binary b;}}");
        String[] columnPath = {"a", "b"};
        ColumnDescriptor c1 = schema.getColumnDescription(columnPath);

        byte[] bytes1 = {0, 1, 2, 3};
        byte[] bytes2 = {2, 3, 4, 5};
        CompressionCodecName codec = CompressionCodecName.UNCOMPRESSED;

        // include statics using update()
        IntStatistics stats = new IntStatistics(); // or BinaryStatistics
        stats.updateStats(1);
        stats.updateStats(2);
        stats.updateStats(5);

        ParquetFileWriter w = new ParquetFileWriter(configuration, schema, path);
        w.start();
        w.startBlock(3);
        w.startColumn(c1, 5, codec);
        w.writeDataPage(2, 4, BytesInput.from(bytes1), stats, BIT_PACKED, BIT_PACKED, PLAIN);
        w.writeDataPage(3, 4, BytesInput.from(bytes1), stats, BIT_PACKED, BIT_PACKED, PLAIN);
        w.endColumn();
        w.endBlock();
        w.startBlock(4);
        w.startColumn(c1, 7, codec);
        w.writeDataPage(7, 4, BytesInput.from(bytes2), stats, BIT_PACKED, BIT_PACKED, PLAIN);
        w.endColumn();
        w.endBlock();
        w.end(new HashMap<String, String>());
        return path;
    }

    @Test
    public void testToInternalDataFile() {
        File file = null;
        Path parentPath = null;
        InternalDataFile internalDataFile = null;
        Configuration configuration = new Configuration();

        try {
            file = new File("./", "test.parquet");
            parentPath = createParquetFile(file);
            //statsExtractor toInternalDataFile testing
            internalDataFile = ParquetStatsExtractor.toInternalDataFile(configuration, parentPath);
        } catch (IOException e) {
            System.out.println(e);
        }

        InternalDataFile inputFile =
                InternalDataFile.builder()
                        .physicalPath(file.toString())
                        .columnStats(Collections.emptyList())
                        .fileFormat(FileFormat.APACHE_PARQUET)
                        .lastModified(1234L)
                        .fileSizeBytes(4321L)
                        .recordCount(0)
                        .build();
        Assertions.assertEquals(
                inputFile, internalDataFile);
    }

    @Test
    public void main() {
        testToInternalDataFile();
    }


}