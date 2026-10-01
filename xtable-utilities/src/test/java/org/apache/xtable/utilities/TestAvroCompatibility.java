package org.apache.xtable.utilities;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.junit.jupiter.api.Test;

public class TestAvroCompatibility {
    @Test
    public void testAvroCompatibility() {
        String schema = "{\"type\":\"record\",\"name\":\"HoodieCleanMetadata\",\"namespace\":\"org.apache.hudi.avro.model\",\"fields\":[{\"name\":\"startCleanTime\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"timeTakenInMillis\",\"type\":\"long\"},{\"name\":\"totalFilesDeleted\",\"type\":\"int\"},{\"name\":\"earliestCommitToRetain\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"lastCompletedCommitTimestamp\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"default\":\"\"},{\"name\":\"partitionMetadata\",\"type\":{\"type\":\"map\",\"values\":{\"type\":\"record\",\"name\":\"HoodieCleanPartitionMetadata\",\"fields\":[{\"name\":\"partitionPath\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"policy\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"deletePathPatterns\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}},{\"name\":\"successDeleteFiles\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}},{\"name\":\"failedDeleteFiles\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}},{\"name\":\"isPartitionDeleted\",\"type\":[\"null\",\"boolean\"],\"default\":null}]},\"avro.java.string\":\"String\"}},{\"name\":\"version\",\"type\":[\"int\",\"null\"],\"default\":1},{\"name\":\"bootstrapPartitionMetadata\",\"type\":[\"null\",{\"type\":\"map\",\"values\":\"HoodieCleanPartitionMetadata\",\"avro.java.string\":\"String\",\"default\":null}],\"default\":null}]}";
        Schema avroSchema = new Schema.Parser().parse(schema);
        SpecificData specificData = SpecificData.getForSchema(avroSchema);
    }
}
