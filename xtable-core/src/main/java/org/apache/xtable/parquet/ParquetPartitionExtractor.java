package org.apache.xtable.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.xtable.delta.DeltaPartitionExtractor;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.PartitionTransformType;
import org.apache.xtable.model.stat.PartitionValue;
import org.apache.xtable.model.stat.Range;
import org.apache.xtable.schema.SchemaFieldFinder;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParquetPartitionExtractor {
    private static final ParquetPartitionExtractor INSTANCE = new ParquetPartitionExtractor();
    public static ParquetPartitionExtractor getInstance() {
        return INSTANCE;
    }

    private Map<String, List<String>> getPartitionFromDirectoryStructure(Configuration hadoopConf, String basePath , Map<String ,List<String>> partitions) {

        try {
            FileSystem fs = FileSystem.get(hadoopConf);
            FileStatus[] baseFileStatus = fs.listStatus(new Path(basePath));
            Map<String, List<String>> partitionMap = new HashMap<>(partitions);

            for (FileStatus dirStatus : baseFileStatus) {
                if (dirStatus.isDirectory()) {
                    String partitionPath = dirStatus.getPath().getName();
                    if (partitionPath.contains("=")) {
                        String[] partitionKeyValue = partitionPath.split("=");
                        partitionMap
                                .computeIfAbsent(partitionKeyValue[0], k -> new ArrayList<String>())
                                .add(partitionKeyValue[1]);
                    }
                    getPartitionFromDirectoryStructure(hadoopConf,dirStatus.getPath().toString(),partitionMap);
                }
            }

            System.out.println("Detected partition " + partitionMap);
            return partitionMap;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private List<PartitionValue> getPartitionValue(String basePath , String filePath, InternalSchema schema) {
        System.out.println("Getting partition value for file " + filePath);
        List<PartitionValue> partitionValues = new ArrayList<>();
        Map<String, List<String>> partitionKeys = getPartitionFromDirectoryStructure(basePath);
        java.nio.file.Path base = Paths.get(basePath).normalize();
        java.nio.file.Path file = Paths.get(filePath).normalize();
        java.nio.file.Path relative = base.relativize(file);
        for (Map.Entry<String, List<String>> entry : partitionKeys.entrySet()) {
            String key = entry.getKey();
            List<String> values = entry.getValue();
            for (String value : values) {
                String pathCheck = key + "=" + value;
                if (relative.startsWith(pathCheck)) {
                    System.out.println("Relative " + relative + " " + pathCheck);
                    partitionValues.add(
                            PartitionValue.builder()
                                    .partitionField(
                                            InternalPartitionField.builder()
                                                    .sourceField(SchemaFieldFinder.getInstance().findFieldByPath(schema, key))
                                                    .transformType(PartitionTransformType.VALUE)
                                                    .build())
                                    .range(Range.scalar(value))
                                    .build());
                }
            }
        }

        return partitionValues;
    }
}
