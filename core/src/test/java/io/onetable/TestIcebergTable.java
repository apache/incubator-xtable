package io.onetable;

import io.onetable.delta.TestDeltaHelper;
import lombok.SneakyThrows;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.functions;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class TestIcebergTable implements GenericTable<Row, String> {
  private static final String NAMESPACE = "iceberg";
  private final SparkSession sparkSession;
  private final TestDeltaHelper testDeltaHelper;
  private final String tableFQN;
  private final String tableBasePath;

  @SneakyThrows
  public TestIcebergTable(String name, Path tempDir, SparkSession sparkSession, boolean includeAdditionalColumns) {
    this.sparkSession = sparkSession;
    this.testDeltaHelper = TestDeltaHelper.createTestDataHelper("level", includeAdditionalColumns);
    this.tableFQN = NAMESPACE + "." + name;
    this.tableBasePath = initBasePath(tempDir, name);
    initializeTable();
  }

  private String initBasePath(Path tempDir, String tableName) throws IOException {
    Path basePath = tempDir.resolve(NAMESPACE).resolve(tableName);
    Files.createDirectories(basePath);
    return basePath.toUri().toString();
  }

  @SneakyThrows
  private void initializeTable() {
    sparkSession.emptyDataset(RowEncoder.apply(testDeltaHelper.getTableStructSchema()))
        .writeTo(tableFQN)
        .using("iceberg")
        .create();
  }

  @SneakyThrows
  @Override
  public List<Row> insertRows(int numRows) {
    List<Row> rows = testDeltaHelper.generateRows(numRows);
    Dataset<Row> df = sparkSession.createDataFrame(rows, testDeltaHelper.getTableStructSchema());
    df.writeTo(tableFQN).append();
    return rows;
  }

  @SneakyThrows
  private List<Row> insertRowsForPartition(int numRows, String level) {
    List<Row> rows = testDeltaHelper.generateRowsForSpecificPartition(numRows, 1990, level);
    Dataset<Row> df = sparkSession.createDataFrame(rows, testDeltaHelper.getTableStructSchema());
    df.writeTo(tableFQN).append();
    return rows;
  }

  @Override
  public List<Row> insertRecordsForSpecialPartition(int numRows) {
    return insertRowsForPartition(numRows, SPECIAL_PARTITION_VALUE);
  }

  @Override
  @SneakyThrows
  public void upsertRows(List<Row> rows) {
    List<Row> upserts = testDeltaHelper.transformForUpsertsOrDeletes(rows, true);
    Dataset<Row> upsertDataset =
        sparkSession.createDataFrame(upserts, testDeltaHelper.getTableStructSchema());
    upsertDataset.registerTempTable("upserts");
    sparkSession.sql(String.format(
        "MERGE INTO %s t " +
        "USING (SELECT * FROM upserts) s " +
        "ON t.id = s.id " +
        "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *", tableFQN));
  }

  @Override
  public void deleteRows(List<Row> rows) {
    List<Row> deletes = testDeltaHelper.transformForUpsertsOrDeletes(rows, false);
    List<Integer> ids = deletes.stream().map(row -> row.getInt(0)).collect(Collectors.toList());
    sparkSession.sql(String.format("DELETE FROM %s WHERE id in (%s)", tableFQN, ids.stream().map(Objects::toString).collect(Collectors.joining(","))));
  }

  @Override
  public void deletePartition(String partitionValue) {
    sparkSession.sql(String.format("DELETE FROM %s WHERE level = '%s'", tableFQN, partitionValue));
  }

  @Override
  public void deleteSpecialPartition() {
    deletePartition(SPECIAL_PARTITION_VALUE);
  }

  @Override
  public String getBasePath() {
    return tableBasePath;
  }

  @Override
  public String getOrderByColumn() {
    return "id";
  }

  @Override
  public void close() {

  }

  @Override
  public void reload() {

  }

  @Override
  public List<String> getColumnsToSelect() {
    return Arrays.asList(testDeltaHelper.getTableStructSchema().fieldNames());
  }
}
