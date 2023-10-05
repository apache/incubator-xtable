package io.onetable.hudi;

import org.apache.hudi.common.table.HoodieTableMetaClient;

import io.onetable.model.OneTable;
import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.PartitionTransformType;
import io.onetable.model.storage.TableFormat;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHudiTableManager {

  private static final Configuration CONFIGURATION = new Configuration();
  @TempDir public static Path tempDir;
  private final String tableBasePath = tempDir.resolve(UUID.randomUUID().toString()).toString();

  private final HudiTableManager tableManager = HudiTableManager.of(CONFIGURATION);

  @Test
  void validateTableInitializedCorrectly() {
    String tableName = "testing_123";
    String field1 = "field1";
    String field2 = "field2";
    List<OnePartitionField> inputPartitionFields =
        Arrays.asList(
            OnePartitionField.builder()
                .sourceField(OneField.builder().name(field1).build())
                .transformType(PartitionTransformType.VALUE)
                .build(),
            OnePartitionField.builder()
                .sourceField(OneField.builder().name(field2).build())
                .transformType(PartitionTransformType.VALUE)
                .build());
    OneTable table = OneTable.builder()
        .name(tableName)
        .partitioningFields(inputPartitionFields)
        .basePath(tableBasePath)
        .tableFormat(TableFormat.ICEBERG)
        .build();

    tableManager.initializeHudiTable(table);

    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setBasePath(tableBasePath)
        .setConf(CONFIGURATION)
        .setLoadActiveTimelineOnLoad(false)
        .build();
    assertFalse(metaClient.getTableConfig().populateMetaFields());
    assertEquals(Arrays.asList(field1, field2), Arrays.asList(metaClient.getTableConfig().getPartitionFields().get()));
    assertEquals(tableBasePath, metaClient.getBasePath());
    assertEquals(tableName, metaClient.getTableConfig().getTableName());
  }

  @Test
  void loadExistingTable() {
    HudiTestUtil.initTableAndGetMetaClient(tableBasePath, "timestamp");
    HoodieTableMetaClient metaClient = tableManager.loadTableIfExists(tableBasePath);
    assertTrue(metaClient.getTableConfig().populateMetaFields());
    assertEquals(Collections.singletonList("timestamp"), Arrays.asList(metaClient.getTableConfig().getPartitionFields().get()));
    assertEquals(tableBasePath, metaClient.getBasePath());
    assertEquals("test_table", metaClient.getTableConfig().getTableName());
  }
}
