package io.onetable;

import io.delta.tables.DeltaTable;
import io.onetable.hudi.HudiTestUtil;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hudi.client.HoodieReadClient;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

// define a standard schema.
// have methods for following
// insert records into the table.
// upsert records into the table.
// delete records from the table.
// update schema by adding columns.
public class TestSparkDeltaTable {

   private static final StructType PERSON_SCHEMA = new StructType(new StructField[]{
        new StructField("firstName", DataTypes.StringType, true, Metadata.empty()),
        new StructField("lastName", DataTypes.StringType, true, Metadata.empty()),
        new StructField("address", new StructType(new StructField[]{
            new StructField("street", DataTypes.StringType, true, Metadata.empty()),
            new StructField("city", DataTypes.StringType, true, Metadata.empty()),
            new StructField("zip", DataTypes.StringType, true, Metadata.empty()),
            new StructField("state", DataTypes.StringType, true, Metadata.empty())
        }), true, Metadata.empty())
    });
    @TempDir
    private static Path tempDir;
    private static SparkSession sparkSession;

    private final String tableName;
    private final String basePath;

    public TestSparkDeltaTable() {
      try {
        this.tableName = "some_table";
        this.basePath = initBasePath(tempDir, tableName);
      } catch (IOException ex) {
        throw new UncheckedIOException("Unable initialize Delta spark table", ex);
      }
    }

    @BeforeAll
    public static void setupOnce() {
      sparkSession = SparkSession.builder()
          .appName("TestDeltaTable")
          .master("local[4]")
          .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
          .getOrCreate();
    }

    @AfterAll
    public static void teardown() {
      if (sparkSession != null) {
        sparkSession.close();
      }
    }

    @Test
    public void createTable() {
      sparkSession.sql(
            "CREATE TABLE testTable (" +
            "    id INT," +
            "    firstName STRING," +
            "    lastName STRING,"  +
            "    gender STRING," +
            "    birthDate TIMESTAMP" +
            ") USING DELTA LOCATION '" + basePath + "'"
        );
      sparkSession.sql("INSERT INTO testTable (id, firstName, lastName, gender, birthDate) VALUES (1, 'John', 'Doe', 'Male', '1990-01-01 00:00:00')");
      sparkSession.sql("INSERT INTO testTable (id, firstName, lastName, gender, birthDate) VALUES (2, 'Jane', 'Smith', 'Female', '1992-02-15 10:30:45')");
      sparkSession.sql("INSERT INTO testTable (id, firstName, lastName, gender, birthDate) VALUES (3, 'Robert', 'Johnson', 'Male', '1985-05-20 14:25:30')");
      sparkSession.sql("INSERT INTO testTable (id, firstName, lastName, gender, birthDate) VALUES (4, 'Emily', 'Clark', 'Female', '1998-08-08 12:12:12')");
      int x = 5;
    }

    public void insertRecords() {

    }

    public void upsertRecords(Dataset<Row> upsertRecords, String mergeKey) {

    }

    public void deleteRecords(String condition) {

    }

    public void addColumn(StructField field) {

    }

    // Create the base path and store it for reference
    private String initBasePath(Path tempDir, String tableName) throws IOException {
      // adding v1 to make it different from just table name.
      Path basePath = tempDir.resolve(tableName + "_v1");
      Files.createDirectories(basePath);
      return basePath.toUri().toString();
    }
}

