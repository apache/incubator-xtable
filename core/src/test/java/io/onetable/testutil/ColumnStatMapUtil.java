package io.onetable.testutil;

import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.OneType;
import io.onetable.model.stat.ColumnStat;
import io.onetable.model.stat.Range;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ColumnStatMapUtil {
  public static Map<OneField, ColumnStat> getColumnStatMap() {
    OneField longField =
        OneField.builder()
            .name("long_field")
            .schema(OneSchema.builder().name("long").dataType(OneType.LONG).build())
            .build();
    OneField stringField =
        OneField.builder()
            .name("string_field")
            .schema(OneSchema.builder().name("string").dataType(OneType.STRING).build())
            .build();
    OneField nullStringField =
        OneField.builder()
            .name("null_string_field")
            .schema(OneSchema.builder().name("string").dataType(OneType.STRING).build())
            .build();
    OneField timestampField =
        OneField.builder()
            .name("timestamp_field")
            .schema(
                OneSchema.builder()
                    .name("long")
                    .dataType(OneType.TIMESTAMP)
                    .metadata(
                        Collections.singletonMap(
                            OneSchema.MetadataKey.TIMESTAMP_PRECISION,
                            OneSchema.MetadataValue.MILLIS))
                    .build())
            .build();
    OneField timestampMicrosField =
        OneField.builder()
            .name("timestamp_micros_field")
            .schema(
                OneSchema.builder()
                    .name("long")
                    .dataType(OneType.TIMESTAMP)
                    .metadata(
                        Collections.singletonMap(
                            OneSchema.MetadataKey.TIMESTAMP_PRECISION,
                            OneSchema.MetadataValue.MICROS))
                    .build())
            .build();
    OneField localTimestampField =
        OneField.builder()
            .name("local_timestamp_field")
            .schema(
                OneSchema.builder()
                    .name("long")
                    .dataType(OneType.TIMESTAMP_NTZ)
                    .metadata(
                        Collections.singletonMap(
                            OneSchema.MetadataKey.TIMESTAMP_PRECISION,
                            OneSchema.MetadataValue.MILLIS))
                    .build())
            .build();
    OneField dateField =
        OneField.builder()
            .name("date_field")
            .schema(OneSchema.builder().name("int").dataType(OneType.DATE).build())
            .build();

    OneField arrayLongFieldElement =
        OneField.builder()
            .name(OneField.Constants.ARRAY_ELEMENT_FIELD_NAME)
            .parentPath("array_long_field")
            .schema(OneSchema.builder().name("long").dataType(OneType.LONG).build())
            .build();
    OneField arrayLongField =
        OneField.builder()
            .name("array_long_field")
            .schema(
                OneSchema.builder()
                    .name("array")
                    .dataType(OneType.ARRAY)
                    .fields(Collections.singletonList(arrayLongFieldElement))
                    .build())
            .build();

    OneField mapKeyStringField =
        OneField.builder()
            .name(OneField.Constants.MAP_KEY_FIELD_NAME)
            .parentPath("map_string_long_field")
            .schema(OneSchema.builder().name("map_key").dataType(OneType.STRING).build())
            .build();
    OneField mapValueLongField =
        OneField.builder()
            .name(OneField.Constants.MAP_VALUE_FIELD_NAME)
            .parentPath("map_string_long_field")
            .schema(OneSchema.builder().name("long").dataType(OneType.LONG).build())
            .build();
    OneField mapStringLongField =
        OneField.builder()
            .name("map_string_long_field")
            .schema(
                OneSchema.builder()
                    .name("map")
                    .dataType(OneType.MAP)
                    .fields(Arrays.asList(mapKeyStringField, mapValueLongField))
                    .build())
            .build();

    OneField nestedArrayStringFieldElement =
        OneField.builder()
            .name(OneField.Constants.ARRAY_ELEMENT_FIELD_NAME)
            .parentPath("nested_struct_field.array_string_field")
            .schema(OneSchema.builder().name("string").dataType(OneType.STRING).build())
            .build();
    OneField nestedArrayStringField =
        OneField.builder()
            .name("array_string_field")
            .parentPath("nested_struct_field")
            .schema(
                OneSchema.builder()
                    .name("array")
                    .dataType(OneType.ARRAY)
                    .fields(Collections.singletonList(nestedArrayStringFieldElement))
                    .build())
            .build();

    OneField nestedLongField =
        OneField.builder()
            .name("nested_long_field")
            .parentPath("nested_struct_field")
            .schema(OneSchema.builder().name("long").dataType(OneType.LONG).build())
            .build();

    OneField nestedStructField =
        OneField.builder()
            .name("nested_struct_field")
            .schema(
                OneSchema.builder()
                    .name("nested_struct_field")
                    .dataType(OneType.RECORD)
                    .fields(Arrays.asList(nestedArrayStringField, nestedLongField))
                    .build())
            .build();

    ColumnStat longColumnStats =
        ColumnStat.builder().numNulls(4).range(Range.vector(10L, 20L)).numValues(5).totalSize(123).build();
    ColumnStat stringColumnStats =
        ColumnStat.builder().numNulls(1).range(Range.vector("a", "c")).numValues(6).totalSize(500).build();
    ColumnStat nullStringColumnStats =
        ColumnStat.builder().numNulls(3).range(Range.vector(null, null)).numValues(3).totalSize(0).build();
    ColumnStat timeStampColumnStats =
        ColumnStat.builder()
            .numNulls(105)
            .range(Range.vector(1665263297000L, 1665436097000L))
            .numValues(145)
            .totalSize(999)
            .build();
    ColumnStat timeStampMicrosColumnStats =
        ColumnStat.builder()
            .numNulls(1)
            .range(Range.vector(1665263297000000L, 1665436097000000L))
            .numValues(20)
            .totalSize(400)
            .build();
    ColumnStat localTimeStampColumnStats =
        ColumnStat.builder()
            .numNulls(1)
            .range(Range.vector(1665263297000L, 1665436097000L))
            .numValues(20)
            .totalSize(400)
            .build();
    ColumnStat dateColumnStats =
        ColumnStat.builder().numNulls(250).range(Range.vector(18181, 18547))
            .numValues(300)
            .totalSize(12345)
            .build();
    ColumnStat ignoredColumnStats =
        ColumnStat.builder().numNulls(0).range(Range.scalar("IGNORED")).build();
    ColumnStat arrayLongElementColumnStats =
        ColumnStat.builder().numNulls(2).range(Range.vector(50L, 100L)).build();
    ColumnStat mapKeyStringColumnStats =
        ColumnStat.builder().numNulls(3).range(Range.vector("key1", "key2")).build();
    ColumnStat mapValueLongColumnStats =
        ColumnStat.builder().numNulls(3).range(Range.vector(200L, 300L)).build();
    ColumnStat nestedArrayStringElementColumnStats =
        ColumnStat.builder().numNulls(7).range(Range.vector("nested1", "nested2")).build();
    ColumnStat nestedLongColumnStats =
        ColumnStat.builder().numNulls(4).range(Range.vector(500L, 600L)).build();

    Map<OneField, ColumnStat> columnStatMap = new HashMap<>();
    columnStatMap.put(longField, longColumnStats);
    columnStatMap.put(stringField, stringColumnStats);
    columnStatMap.put(nullStringField, nullStringColumnStats);
    columnStatMap.put(timestampField, timeStampColumnStats);
    columnStatMap.put(timestampMicrosField, timeStampMicrosColumnStats);
    columnStatMap.put(localTimestampField, localTimeStampColumnStats);
    columnStatMap.put(dateField, dateColumnStats);
    columnStatMap.put(arrayLongField, ignoredColumnStats);
    columnStatMap.put(arrayLongFieldElement, arrayLongElementColumnStats);
    columnStatMap.put(mapStringLongField, ignoredColumnStats);
    columnStatMap.put(mapKeyStringField, mapKeyStringColumnStats);
    columnStatMap.put(mapValueLongField, mapValueLongColumnStats);
    columnStatMap.put(nestedStructField, ignoredColumnStats);
    columnStatMap.put(nestedArrayStringField, ignoredColumnStats);
    columnStatMap.put(nestedArrayStringFieldElement, nestedArrayStringElementColumnStats);
    columnStatMap.put(nestedLongField, nestedLongColumnStats);
    return columnStatMap;
  }

}
