import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.schema.*;


import java.util.ArrayList;
import java.util.List;


public class TestParquetSchemaExtractor {
    static final GroupType mapGroupType = new GroupType(Type.Repetition.REPEATED, "key_value",
            new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "fakekey"),
            new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "fakevalue"));
    static final GroupType groupType = new GroupType(Type.Repetition.REQUIRED, "my_map", OriginalType.MAP, mapGroupType);
    static final GroupType mapGroupType2 = new GroupType(Type.Repetition.REPEATED, "key_value",
            new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "fakekey"),
            groupType);
    static final GroupType groupType2 = new GroupType(Type.Repetition.REQUIRED, "my_map", OriginalType.MAP, mapGroupType2);
    static final MessageType messageType = new MessageType("schema", groupType2);
    public static void main(String[] args) {
        generateParquetFileFor();
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

        for(int i = 1; i <= 4; i++) {
            Group mapGroup = new SimpleGroup(mapGroupType);
            mapGroup.add("fakekey", i*i);
            mapGroup.add("fakevalue", i*i*i);
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
}