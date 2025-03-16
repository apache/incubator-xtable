package org.apache.xtable.parquet;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.schema.*;
import org.apache.parquet.schema.Type.Repetition;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.List;


public class TestParquetSchemaExtractor {
    static final GroupType mapGroupType = new GroupType(Type.Repetition.REPEATED, "key_value",
            new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "fakekey"),
            new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.FLOAT, "fakevalue"));
    static final GroupType groupType = new GroupType(Type.Repetition.REQUIRED, "my_map", OriginalType.MAP, mapGroupType);
    static final GroupType mapGroupType2 = new GroupType(Type.Repetition.REPEATED, "key_value",
            new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "fakekey"),
            groupType);
    static final GroupType groupType2 = new GroupType(Type.Repetition.REQUIRED, "my_map", OriginalType.MAP, mapGroupType2);
    static final MessageType messageType = new MessageType("schema", groupType2);
    private static final ParquetSchemaExtractor SCHEMA_EXTRACTOR =
            ParquetSchemaExtractor.getInstance();

    public static void main(String[] args) {
        //generateParquetFileFor();
        testPrimitiveTypes();
    }

    private static void generateParquetFileFor() {
        try {
            MessageType schema = parseSchema();
            Path path = new Path("test.parquet");

            List<Group> recordList = generateRecords();

            ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(path);
            try (ParquetWriter<Group> writer = builder.withType(schema).build()) {
                for (Group record : recordList) {
                    writer.write(record);
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace(System.out);
        }
    }

    private static MessageType parseSchema() {
        return messageType;
    }

    private static List<Group> generateRecords() {

        List<Group> recordList = new ArrayList<>();

        for (int i = 1; i <= 4; i++) {
            Group mapGroup = new SimpleGroup(mapGroupType);
            mapGroup.add("fakekey", i * i);
            mapGroup.add("fakevalue", i * i * i);
            Group group = new SimpleGroup(groupType);
            group.add("key_value", mapGroup);
            Group mapGroup2 = new SimpleGroup(mapGroupType2);
            mapGroup2.add("fakekey", i);
            mapGroup2.add("my_map", group);
            Group group2 = new SimpleGroup(groupType2);
            group2.add("key_value", mapGroup2);
            Group mType = new SimpleGroup(messageType);
            mType.add("my_map", group2);
            recordList.add(mType);
        }

        return recordList;
    }

    @Test
    public void testPrimitiveTypes() {
       /* Map<InternalSchema.MetadataKey, Object> requiredEnumMetadata =
                Collections.singletonMap(
                        InternalSchema.MetadataKey.ENUM_VALUES, Arrays.asList("ONE", "TWO"));
        Map<InternalSchema.MetadataKey, Object> optionalEnumMetadata =
                Collections.singletonMap(
                        InternalSchema.MetadataKey.ENUM_VALUES, Arrays.asList("THREE", "FOUR"));*/
        InternalSchema primitive1 = InternalSchema.builder()
                .name("integer");
                .dataType(InternalType.INT);
        InternalSchema primitive2 = InternalSchema.builder()
                .name("string");
                .dataType(InternalType.STRING);


/*        InternalSchema schemaWithPrimitiveTypes =
                InternalSchema.builder()
                        .dataType(InternalType.RECORD)
                        .fields(
                                Arrays.asList(
                                        InternalField.builder()
                                                .name("int")
                                                .schema(
                                                        InternalSchema.builder()
                                                                .name("REQUIRED_int")
                                                                .dataType(InternalType.INT)
                                                                .isNullable(false)
                                                                .metadata(null)
                                                                .build())
                                                .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
                                                .build(),
                                        InternalField.builder()
                                                .name("float")
                                                .schema(
                                                        InternalSchema.builder()
                                                                .name("REQUIRED_double")
                                                                .dataType(InternalType.FLOAT)
                                                                .isNullable(true)
                                                                .metadata(null)
                                                                .build())
                                                .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
                                                .build()
                                ))
                        .build();*/
        //Type expectedSchema = mapGroupType;
        MessageType integerPrimitiveType = MessageType(REQUIRED,PrimitiveType(Type.Repetition repetition, INT32, "integer") );
        Assertions.assertEquals(
                primitive1, SCHEMA_EXTRACTOR.toInternalSchema(integerPrimitiveType, null, null));
        //assertTrue(TestParquetSchemaExtractor.mapGroupType.equals(SCHEMA_EXTRACTOR.toInternalSchema(schemaWithPrimitiveTypes)));
    }
}