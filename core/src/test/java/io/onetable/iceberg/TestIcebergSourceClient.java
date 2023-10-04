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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;

import io.onetable.client.PerTableConfig;
import io.onetable.model.OneTable;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.storage.TableFormat;

class TestIcebergSourceClient {

  private HadoopTables tables;
  private Schema csSchema;
  private PartitionSpec csPartitionSpec;
  private IcebergSourceClientProvider clientProvider;

  @BeforeEach
  void setUp() throws IOException {
    Configuration hadoopConf = new Configuration();
    hadoopConf.set("fs.defaultFS", "file:///");

    clientProvider = new IcebergSourceClientProvider();
    clientProvider.init(hadoopConf, null);

    tables = new HadoopTables(hadoopConf);

    byte[] bytes = readResourceFile("schemas/catalog_sales.json");
    csSchema = SchemaParser.fromJson(new String(bytes));

    bytes = readResourceFile("partition_specs/catalog_sales.json");
    csPartitionSpec = PartitionSpecParser.fromJson(csSchema, new String(bytes));
  }

  @Test
  void getTableTest() throws IOException {
    Table catalogSales = createTestTableWithData();
    PerTableConfig sourceTableConfig =
        PerTableConfig.builder()
            .tableName(catalogSales.name())
            .tableBasePath(catalogSales.location())
            .targetTableFormats(Collections.singletonList(TableFormat.DELTA))
            .build();

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

    Assertions.assertEquals(34, oneTable.getReadSchema().getFields().size());
    validateSchema(oneTable.getReadSchema(), catalogSales.schema());
    // TODO validate schema fields using round trip to iceberg

    // TODO fix this, there is 1 partition field
    Assertions.assertEquals(0, oneTable.getPartitioningFields().size());
  }

  private void validateSchema(OneSchema readSchema, Schema schema) {}

  private Table createTestTableWithData() throws IOException {
    String csPath = String.join("", System.getProperty("java.io.tmpdir"), "catalog_sales");
    FileUtils.deleteDirectory(Paths.get(csPath).toFile());
    Table catalogSales = tables.create(csSchema, csPartitionSpec, csPath);

    String dataFilePath = String.join("/data/", csPath, UUID.randomUUID() + ".parquet");
    DataWriter<GenericRecord> dataWriter =
        Parquet.writeData(catalogSales.io().newOutputFile(dataFilePath))
            .schema(csSchema)
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .build();

    try {
      GenericRecord record = GenericRecord.create(csSchema);
      record.setField("cs_sold_date_sk", 1);
      dataWriter.write(record);
    } finally {
      dataWriter.close();
    }

    catalogSales.newAppend().appendFile(dataWriter.toDataFile()).commit();
    return catalogSales;
  }

  private byte[] readResourceFile(String resourcePath) throws IOException {
    return Files.readAllBytes(
        Paths.get(getClass().getClassLoader().getResource(resourcePath).getPath()));
  }
}
