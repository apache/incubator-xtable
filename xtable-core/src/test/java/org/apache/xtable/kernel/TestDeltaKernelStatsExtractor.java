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
package org.apache.xtable.kernel;

import static org.apache.xtable.testutil.ColumnStatMapUtil.getColumnStats;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.*;

import io.delta.kernel.data.Row;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import org.apache.xtable.delta.DeltaStatsExtractor;
import org.junit.jupiter.api.Test;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.stat.ColumnStat;
import org.apache.xtable.model.stat.FileStats;
import org.apache.xtable.model.stat.Range;
import org.apache.xtable.testutil.ColumnStatMapUtil;
import io.delta.kernel.statistics.DataFileStatistics;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.types.StructType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Literal;


public class TestDeltaKernelStatsExtractor {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    public void testDeltaStats() throws JsonProcessingException {
        InternalSchema schema = ColumnStatMapUtil.getSchema();

        List<ColumnStat> columnStats = getColumnStats();

        String actualStats =
                DeltaKernelStatsExtractor.getInstance().convertStatsToDeltaFormat(schema, 50L, columnStats);
        Map<String, Object> actualStatsMap = MAPPER.readValue(actualStats, HashMap.class);
        assertEquals(50, actualStatsMap.get("numRecords"));
        Map<String, Object> minValueStatsMap =
                (HashMap<String, Object>) actualStatsMap.get("minValues");
        assertEquals(10, minValueStatsMap.get("long_field"));
        assertEquals("a", minValueStatsMap.get("string_field"));
        assertEquals(null, minValueStatsMap.get("null_string_field"));
        assertEquals("2022-10-08 21:08:17", minValueStatsMap.get("timestamp_field"));
        assertEquals("2022-10-08 21:08:17", minValueStatsMap.get("timestamp_micros_field"));
        assertEquals(1.23, minValueStatsMap.get("float_field"));
        assertEquals(1.23, minValueStatsMap.get("double_field"));
        assertEquals(1.0, minValueStatsMap.get("decimal_field"));
        // TOD0: Local timestamp depends on env where it is run, it is non determinstic and this has to
        // be computed dynamically.
        // assertEquals("2022-10-08 14:08:17", minValueStatsMap.get("local_timestamp_field"));
        assertEquals("2019-10-12", minValueStatsMap.get("date_field"));
        Map<String, Object> nestedMapInMinValueStatsMap =
                (HashMap<String, Object>) minValueStatsMap.get("nested_struct_field");
        assertEquals(500, nestedMapInMinValueStatsMap.get("nested_long_field"));

        Map<String, Object> maxValueStatsMap =
                (HashMap<String, Object>) actualStatsMap.get("maxValues");
        assertEquals(20, maxValueStatsMap.get("long_field"));
        assertEquals("c", maxValueStatsMap.get("string_field"));
        assertEquals(null, maxValueStatsMap.get("null_string_field"));
        assertEquals("2022-10-10 21:08:17", maxValueStatsMap.get("timestamp_field"));
        assertEquals("2022-10-10 21:08:17", maxValueStatsMap.get("timestamp_micros_field"));
        // TOD0: Local timestamp depends on env where it is run, it is non determinstic and this has to
        // be computed dynamically.
        // assertEquals("2022-10-10 14:08:17", maxValueStatsMap.get("local_timestamp_field"));
        assertEquals("2020-10-12", maxValueStatsMap.get("date_field"));
        assertEquals(6.54321, maxValueStatsMap.get("float_field"));
        assertEquals(6.54321, maxValueStatsMap.get("double_field"));
        assertEquals(2.0, maxValueStatsMap.get("decimal_field"));
        Map<String, Object> nestedMapInMaxValueStatsMap =
                (HashMap<String, Object>) maxValueStatsMap.get("nested_struct_field");
        assertEquals(600, nestedMapInMaxValueStatsMap.get("nested_long_field"));

        Map<String, Object> nullValueStatsMap =
                (HashMap<String, Object>) actualStatsMap.get("nullCount");
        assertEquals(4, nullValueStatsMap.get("long_field"));
        assertEquals(1, nullValueStatsMap.get("string_field"));

        assertEquals(3, nullValueStatsMap.get("null_string_field"));
        assertEquals(105, nullValueStatsMap.get("timestamp_field"));
        assertEquals(1, nullValueStatsMap.get("timestamp_micros_field"));
        assertEquals(1, nullValueStatsMap.get("local_timestamp_field"));
        assertEquals(250, nullValueStatsMap.get("date_field"));
        assertEquals(2, nullValueStatsMap.get("float_field"));
        assertEquals(3, nullValueStatsMap.get("double_field"));
        assertEquals(1, nullValueStatsMap.get("decimal_field"));
        Map<String, Object> nestedMapInNullCountMap =
                (HashMap<String, Object>) nullValueStatsMap.get("nested_struct_field");
        assertEquals(4, nestedMapInNullCountMap.get("nested_long_field"));

    }
    @Test
    void roundTripStatsConversion() throws IOException {
        InternalSchema schema = ColumnStatMapUtil.getSchema();
        List<InternalField> fields = schema.getAllFields();
        List<ColumnStat> columnStats = getColumnStats();
        Map<String, String> partitionValues = new HashMap<>();
        partitionValues.put("a", "1");

        long numRecords1 = 50L;
        String stats =
                DeltaKernelStatsExtractor.getInstance()
                        .convertStatsToDeltaFormat(schema, numRecords1, columnStats);
        JsonNode root = MAPPER.readTree(stats);
        // Extract numRecords
        long numRecords = root.get("numRecords").asLong();

        // Extract and convert minValues
        Map<Column, Literal> minValues = parseValues(root.get("minValues"));

        // Extract and convert maxValues
        Map<Column, Literal> maxValues = parseValues(root.get("maxValues"));

        Map<Column, Long> nullCount = parseNullCount(root.get("nullCounts"));

        DataFileStatistics filestats = new DataFileStatistics(numRecords, minValues, maxValues, nullCount);


        Row addFileRow = AddFile.createAddFileRow(
        null,
        "test/path",
        VectorUtils.stringStringMapValue(partitionValues),
        0,
        0,
        true,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),Optional.empty(), Optional.of(filestats)
);

        AddFile addFile = new AddFile(addFileRow);
        DeltaKernelStatsExtractor extractor = DeltaKernelStatsExtractor.getInstance();
        FileStats actual = extractor.getColumnStatsForFile(addFile, fields);
    }

    private Map<Column, Literal> parseValues(JsonNode valuesNode) {
        Map<Column, Literal> values = new HashMap<>();
        if (valuesNode == null || valuesNode.isNull()) {
            return values;
        }

        Iterator<Map.Entry<String, JsonNode>> fields = valuesNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String columnName = entry.getKey();
            JsonNode valueNode = entry.getValue();
            values.put(new Column(columnName), convertToLiteral(valueNode));
        }
        return values;
    }

    private Literal convertToLiteral(JsonNode valueNode) {
        System.out.println("ValueNode: " + valueNode);
        if (valueNode.isNull()) {
            return Literal.ofNull(StringType.STRING);
        }
         else if (valueNode.isTextual()) {
            return Literal.ofString(valueNode.asText());
        } else if (valueNode.isInt()) {
            return Literal.ofInt(valueNode.asInt());
        } else if (valueNode.isLong()) {
            return Literal.ofLong(valueNode.asLong());
        } else if (valueNode.isDouble()) {
            return Literal.ofDouble(valueNode.asDouble());
        } else if (valueNode.isFloat()) {
            return Literal.ofFloat((float) valueNode.asDouble());
        } else if (valueNode.isBoolean()) {
            return Literal.ofBoolean(valueNode.asBoolean());
        } else if (valueNode.isObject()) {
            // Handle nested objects
            return Literal.ofString(valueNode.toString());
        } else {
            throw new IllegalArgumentException("Unsupported JSON value type: " + valueNode.getNodeType());
        }
    }

    private Map<Column, Long> parseNullCount(JsonNode nullCountNode) {
        Map<Column, Long> nullCounts = new HashMap<>();
        if (nullCountNode == null || nullCountNode.isNull()) {
            return nullCounts;
        }

        Iterator<Map.Entry<String, JsonNode>> fields = nullCountNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String columnName = entry.getKey();
            JsonNode countNode = entry.getValue();
            if (countNode.isNumber()) {
                nullCounts.put(new Column(columnName), countNode.asLong());
            } else if (countNode.isObject()) {
                // Handle nested null counts for nested fields
                // You might want to handle this differently based on your needs
                nullCounts.put(new Column(columnName), 0L);
            }
        }
        return nullCounts;
    }
    private List<InternalField> getSchemaFields() {
        return Arrays.asList(
                InternalField.builder()
                        .name("top_level_string")
                        .schema(InternalSchema.builder().dataType(InternalType.STRING).build())
                        .build(),
                InternalField.builder()
                        .name("nested")
                        .schema(InternalSchema.builder().dataType(InternalType.RECORD).build())
                        .build(),
                InternalField.builder()
                        .name("int_field")
                        .parentPath("nested")
                        .schema(InternalSchema.builder().dataType(InternalType.INT).build())
                        .build(),
                InternalField.builder()
                        .name("double_nesting")
                        .parentPath("nested")
                        .schema(InternalSchema.builder().dataType(InternalType.RECORD).build())
                        .build(),
                InternalField.builder()
                        .name("double_field")
                        .parentPath("nested.double_nesting")
                        .schema(InternalSchema.builder().dataType(InternalType.DOUBLE).build())
                        .build(),
                InternalField.builder()
                        .name("top_level_int")
                        .schema(InternalSchema.builder().dataType(InternalType.INT).build())
                        .build());
    }


}
