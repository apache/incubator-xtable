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
 
package io.onetable.iceberg;

import static org.mockito.Mockito.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.iceberg.*;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

import io.onetable.client.PerTableConfig;
import io.onetable.model.OneSnapshot;
import io.onetable.model.OneTable;
import io.onetable.model.TableChange;
import io.onetable.model.schema.*;
import io.onetable.model.stat.Range;
import io.onetable.model.storage.FileFormat;
import io.onetable.model.storage.OneDataFile;
import io.onetable.model.storage.OneDataFiles;
import io.onetable.model.storage.TableFormat;

class TestIcebergSourceClient {

  private IcebergTableManager tableManager;
  private Schema csSchema;
  private PartitionSpec csPartitionSpec;
  private IcebergSourceClientProvider clientProvider;
  private Configuration hadoopConf;

  @BeforeEach
  void setUp() throws IOException {
    hadoopConf = new Configuration();
    hadoopConf.set("fs.defaultFS", "file:///");

    clientProvider = new IcebergSourceClientProvider();
    clientProvider.init(hadoopConf, null);

    tableManager = IcebergTableManager.of(hadoopConf);

    byte[] bytes = readResourceFile("schemas/catalog_sales.json");
    csSchema = SchemaParser.fromJson(new String(bytes));

    bytes = readResourceFile("partition_specs/catalog_sales.json");
    csPartitionSpec = PartitionSpecParser.fromJson(csSchema, new String(bytes));
  }

  @Test
  void getTableTest(@TempDir Path workingDir) throws IOException {
    Table catalogSales = createTestTableWithData(workingDir.toString());
    PerTableConfig sourceTableConfig = getPerTableConfig(catalogSales);

    IcebergSourceClient client = clientProvider.getSourceClientInstance(sourceTableConfig);

    Snapshot snapshot = catalogSales.currentSnapshot();
    OneTable oneTable = client.getTable(snapshot);
    Assertions.assertNotNull(oneTable);
    Assertions.assertEquals(TableFormat.ICEBERG, oneTable.getTableFormat());
    Assertions.assertTrue(oneTable.getName().endsWith("catalog_sales"));
    Assertions.assertEquals(catalogSales.location(), oneTable.getBasePath());
    Assertions.assertEquals(
        snapshot.timestampMillis(), oneTable.getLatestCommitTime().toEpochMilli());
    Assertions.assertNotNull(oneTable.getReadSchema());

    Assertions.assertEquals(7, oneTable.getReadSchema().getFields().size());
    validateSchema(oneTable.getReadSchema(), catalogSales.schema());

    Assertions.assertEquals(1, oneTable.getPartitioningFields().size());
    OneField partitionField = oneTable.getPartitioningFields().get(0).getSourceField();
    Assertions.assertEquals("cs_sold_date_sk", partitionField.getName());
    Assertions.assertEquals(7, partitionField.getFieldId());
    Assertions.assertEquals(
        PartitionTransformType.VALUE, oneTable.getPartitioningFields().get(0).getTransformType());
  }

  @Test
  public void getSchemaCatalogTest(@TempDir Path workingDir) throws IOException {
    Table catalogSales = createTestTableWithData(workingDir.toString());
    Snapshot iceCurrentSnapshot = catalogSales.currentSnapshot();

    // add extra schema to test current schema is returned
    catalogSales.updateSchema().addColumn("new_column", Types.IntegerType.get()).commit();
    Assertions.assertEquals(2, catalogSales.schemas().size());
    Assertions.assertEquals(0, iceCurrentSnapshot.schemaId());

    PerTableConfig sourceTableConfig = getPerTableConfig(catalogSales);

    IcebergSourceClient client = clientProvider.getSourceClientInstance(sourceTableConfig);
    IcebergSourceClient spyClient = spy(client);

    SchemaCatalog schemaCatalog = spyClient.getSchemaCatalog(null, iceCurrentSnapshot);
    Assertions.assertNotNull(schemaCatalog);
    Map<SchemaVersion, OneSchema> schemas = schemaCatalog.getSchemas();
    Assertions.assertEquals(1, schemas.size());
    SchemaVersion expectedSchemaVersion = new SchemaVersion(iceCurrentSnapshot.schemaId(), "");
    OneSchema irSchemaOfCommit = schemas.get(expectedSchemaVersion);
    Assertions.assertNotNull(irSchemaOfCommit);
    validateSchema(irSchemaOfCommit, catalogSales.schemas().get(iceCurrentSnapshot.schemaId()));
  }

  @Test
  public void testGetCurrentSnapshot(@TempDir Path workingDir) throws IOException {
    Table catalogSales = createTestTableWithData(workingDir.toString());
    Snapshot iceCurrentSnapshot = catalogSales.currentSnapshot();

    PerTableConfig sourceTableConfig = getPerTableConfig(catalogSales);

    IcebergDataFileExtractor spyDataFileExtractor = spy(IcebergDataFileExtractor.builder().build());
    IcebergPartitionValueConverter spyPartitionConverter =
        spy(IcebergPartitionValueConverter.getInstance());

    IcebergSourceClient spyClient =
        spy(
            IcebergSourceClient.builder()
                .hadoopConf(hadoopConf)
                .sourceTableConfig(sourceTableConfig)
                .dataFileExtractor(spyDataFileExtractor)
                .partitionConverter(spyPartitionConverter)
                .build());

    OneSnapshot oneSnapshot = spyClient.getCurrentSnapshot();
    Assertions.assertNotNull(oneSnapshot);
    Assertions.assertEquals(
        String.valueOf(iceCurrentSnapshot.snapshotId()), oneSnapshot.getVersion());
    Assertions.assertNotNull(oneSnapshot.getTable());
    verify(spyClient, times(1)).getTable(iceCurrentSnapshot);
    verify(spyClient, times(1)).getSchemaCatalog(oneSnapshot.getTable(), iceCurrentSnapshot);
    verify(spyPartitionConverter, times(5)).toOneTable(any(), any());
    verify(spyDataFileExtractor, times(5)).fromIceberg(any(), any(), any());

    Assertions.assertNotNull(oneSnapshot.getDataFiles());
    List<OneDataFile> dataFileChunks = oneSnapshot.getDataFiles().getFiles();
    Assertions.assertEquals(5, dataFileChunks.size());
    for (OneDataFile dataFilesChunk : dataFileChunks) {
      Assertions.assertInstanceOf(OneDataFiles.class, dataFilesChunk);
      OneDataFiles oneDataFiles = (OneDataFiles) dataFilesChunk;
      Assertions.assertEquals(1, oneDataFiles.getFiles().size());
      OneDataFile oneDataFile = oneDataFiles.getFiles().get(0);
      Assertions.assertEquals(FileFormat.APACHE_PARQUET, oneDataFile.getFileFormat());
      Assertions.assertEquals(1, oneDataFile.getRecordCount());
      Assertions.assertTrue(oneDataFile.getPhysicalPath().startsWith(workingDir.toString()));

      Map<OnePartitionField, Range> partitionValues = oneDataFile.getPartitionValues();
      Assertions.assertEquals(1, partitionValues.size());
      Map.Entry<OnePartitionField, Range> partitionEntry =
          partitionValues.entrySet().iterator().next();
      Assertions.assertEquals(
          "cs_sold_date_sk", partitionEntry.getKey().getSourceField().getName());
      // TODO generate test with column stats
      Assertions.assertEquals(0, oneDataFile.getColumnStats().size());
    }
  }

  @Test
  public void testGetTableChangeForCommit(@TempDir Path workingDir) throws IOException {
    Table catalogSales = createTestTableWithData(workingDir.toString());
    IcebergSourceClient sourceClient = getIcebergSourceClient(catalogSales);

    Snapshot initialCreateSnapshot = catalogSales.currentSnapshot();
    Assertions.assertEquals(
        5, StreamSupport.stream(catalogSales.newScan().planFiles().spliterator(), false).count());
    TableChange tableChange = sourceClient.getTableChangeForCommit(initialCreateSnapshot);
    validateTableChangeDiffSize(tableChange, 5, 0);

    // add new commit and validate table change for old and new commit is correctly returned
    catalogSales
        .newDelete()
        .deleteFromRowFilter(Expressions.lessThan("cs_sold_date_sk", 3))
        .commit();
    Snapshot rowsDeletedSnapshot = catalogSales.currentSnapshot();
    Assertions.assertNotEquals(initialCreateSnapshot, rowsDeletedSnapshot);
    sourceClient.refreshSourceTable();
    tableChange = sourceClient.getTableChangeForCommit(rowsDeletedSnapshot);
    validateTableChangeDiffSize(tableChange, 0, 3);
    tableChange = sourceClient.getTableChangeForCommit(initialCreateSnapshot);
    validateTableChangeDiffSize(tableChange, 5, 0);
    Assertions.assertEquals(
        2, StreamSupport.stream(catalogSales.newScan().planFiles().spliterator(), false).count());
  }

  private static void validateTableChangeDiffSize(
      TableChange tableChange, int addedFiles, int removedFiles) {
    Assertions.assertEquals(addedFiles, tableChange.getFilesDiff().getFilesAdded().size());
    Assertions.assertEquals(removedFiles, tableChange.getFilesDiff().getFilesRemoved().size());
  }

  private void validateSchema(OneSchema readSchema, Schema expectedSchema) {
    IcebergSchemaExtractor schemaExtractor = IcebergSchemaExtractor.getInstance();
    Schema result = schemaExtractor.toIceberg(readSchema);

    Assertions.assertEquals(result.columns().size(), expectedSchema.columns().size());

    Map<String, Types.NestedField> columnMap =
        result.columns().stream().collect(Collectors.toMap(Types.NestedField::name, f -> f));

    for (Types.NestedField expectedField : expectedSchema.columns()) {
      Types.NestedField column = columnMap.get(expectedField.name());
      Assertions.assertNotNull(column);
      Assertions.assertEquals(expectedField.fieldId(), column.fieldId());
      Assertions.assertEquals(expectedField.type(), column.type());
      Assertions.assertEquals(expectedField.isOptional(), column.isOptional());

      // TODO: fix this
      //      Assertions.assertEquals(expectedField.doc(), column.doc());
      //      Assertions.assertEquals(expectedField.getOrdinal(), column.getOrdinal());
      //      Assertions.assertEquals(expectedField.getTransform(), column.getTransform());
    }
  }

  private Table createTestTableWithData(String workingDir) throws IOException {
    String csPath = Paths.get(workingDir, "catalog_sales").toString();
    Table catalogSales =
        tableManager.getOrCreateTable(null, null, csPath, csSchema, csPartitionSpec);

    AppendFiles appendFiles = catalogSales.newAppend();

    for (int numFile = 0; numFile < 5; numFile++) {
      // The test creates one file in each partition
      String dataFilePath =
          String.join("/", catalogSales.location(), "data", UUID.randomUUID() + ".parquet");
      PartitionData partitionInfo = new PartitionData(csPartitionSpec.partitionType());
      partitionInfo.set(0, numFile);
      DataWriter<GenericRecord> dataWriter =
          Parquet.writeData(catalogSales.io().newOutputFile(dataFilePath))
              .schema(csSchema)
              .createWriterFunc(GenericParquetWriter::buildWriter)
              .overwrite()
              .withSpec(csPartitionSpec)
              .withPartition(partitionInfo)
              .build();

      try {
        GenericRecord record = GenericRecord.create(csSchema);
        record.setField("cs_sold_date_sk", numFile);
        dataWriter.write(record);
      } finally {
        dataWriter.close();
      }

      appendFiles.appendFile(dataWriter.toDataFile());
    }
    appendFiles.commit();

    return catalogSales;
  }

  private IcebergSourceClient getIcebergSourceClient(Table catalogSales) {
    PerTableConfig tableConfig = getPerTableConfig(catalogSales);

    return IcebergSourceClient.builder()
        .hadoopConf(hadoopConf)
        .sourceTableConfig(tableConfig)
        .build();
  }

  private static PerTableConfig getPerTableConfig(Table catalogSales) {
    return PerTableConfig.builder()
        .tableName(catalogSales.name())
        .tableBasePath(catalogSales.location())
        .targetTableFormats(Collections.singletonList(TableFormat.DELTA))
        .build();
  }

  private byte[] readResourceFile(String resourcePath) throws IOException {
    return Files.readAllBytes(
        Paths.get(getClass().getClassLoader().getResource(resourcePath).getPath()));
  }
}
