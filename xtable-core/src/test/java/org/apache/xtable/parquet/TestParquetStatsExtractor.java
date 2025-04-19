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

import static org.apache.parquet.column.Encoding.BIT_PACKED;
import static org.apache.parquet.column.Encoding.PLAIN;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import lombok.Builder;

import org.apache.hadoop.conf.Configuration;
import java.nio.file.Paths;

import java.nio.file.Files;
//import java.nio.file.Path;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.*;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.stat.ColumnStat;
import org.apache.xtable.model.stat.Range;
import org.apache.xtable.model.storage.FileFormat;
import org.apache.xtable.model.storage.InternalDataFile;

import org.junit.jupiter.api.io.TempDir;

public class TestParquetStatsExtractor {
    @TempDir
    static java.nio.file.Path tempDir = Paths.get("./");

    @Builder.Default
    private static final ParquetSchemaExtractor schemaExtractor =
            ParquetSchemaExtractor.getInstance();

    public static List<ColumnStat> initFileTest(File file) throws IOException {
        // create the parquet file by parsing a schema
        Path path = new Path(file.toURI());
        Configuration configuration = new Configuration();

        MessageType schema =
                MessageTypeParser.parseMessageType("message m { required group a {required binary b;}}");
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

        // to simplify the test we keep the same stats for both columns
        ParquetFileWriter w = new ParquetFileWriter(configuration, schema, path);
        w.start();
        w.startBlock(3);
        w.startColumn(c1, 5, codec);
        w.writeDataPage(2, 4, BytesInput.from(bytes1), stats, BIT_PACKED, BIT_PACKED, PLAIN);
        w.writeDataPage(3, 4, BytesInput.from(bytes1), stats, BIT_PACKED, BIT_PACKED, PLAIN);
        w.endColumn();
        w.endBlock();
        w.startBlock(4);
        w.startColumn(c1, 8, codec);
        w.writeDataPage(7, 4, BytesInput.from(bytes2), stats, BIT_PACKED, BIT_PACKED, PLAIN);
        w.endColumn();
        w.endBlock();
        w.end(new HashMap<String, String>());

        // reconstruct the stats for the InternalDataFile testing object
        byte[] minStat = stats.getMinBytes();
        byte[] maxStat = stats.getMaxBytes();
        PrimitiveType primitiveType =
                new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "b");
        List<Integer> col1NumValTotSize = new ArrayList<>(Arrays.asList(5, 8));
        List<Integer> col2NumValTotSize = new ArrayList<>(Arrays.asList(54, 27));
        List<ColumnStat> testColumnStats = new ArrayList<>();
        String[] columnDotPath = {"a.b", "a.b"};
        for (int i = 0; i < columnDotPath.length; i++) {
            testColumnStats.add(
                    ColumnStat.builder()
                            .field(
                                    InternalField.builder()
                                            .name(primitiveType.getName())
                                            .parentPath(null)
                                            .schema(schemaExtractor.toInternalSchema(primitiveType, columnDotPath[i]))
                                            .build())
                            .numValues(col1NumValTotSize.get(i))
                            .totalSize(col2NumValTotSize.get(i))
                            .range(Range.vector(minStat, maxStat))
                            .build());
        }

        return testColumnStats; // new ParquetFileReader(configuration, path, w.getFooter());
    }

    @Test
    public void testToInternalDataFile() throws IOException {


        ParquetFileReader fileReader = null;
        InternalDataFile internalDataFile = null;
        Configuration configuration = new Configuration();
        List<ColumnStat> testColumnStats = new ArrayList<>();
        java.nio.file.Path path = tempDir.resolve("parquet-test-files");
        File file = path.toFile();
        file.deleteOnExit();
        //file = new File("./", "test.parquet");
        // fileReader = createParquetFile(file);
        //testColumnStats = initFileTest(file);
        testColumnStats = initFileTest(file);
        Path hadoopPath = new Path(file.toURI());
        // statsExtractor toInternalDataFile testing
        internalDataFile = ParquetStatsExtractor.toInternalDataFile(configuration, hadoopPath);
        //System.out.println(file.getAbsolutePath());
        InternalDataFile testInternalFile =
                InternalDataFile.builder()
                        .physicalPath(
                                "file:/C:/Users/slims/Downloads/XTable/incubator-xtable/xtable-core/parquet-test-files") // TODO hard coded path to file method
                        .columnStats(testColumnStats)
                        .fileFormat(FileFormat.APACHE_PARQUET)
                        .lastModified(file.lastModified())
                        .fileSizeBytes(file.length())
                        .recordCount(8) // from start blocks
                        .build();

        Assertions.assertEquals(true, testInternalFile.equals(internalDataFile));
    }

    public void main() throws IOException {
        testToInternalDataFile();
    }
}
