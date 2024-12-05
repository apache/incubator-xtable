package org.apache.xtable.parquet;

import lombok.Builder;
import lombok.NonNull;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.xtable.avro.AvroSchemaConverter;

import org.apache.xtable.model.*;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.PartitionTransformType;
import org.apache.xtable.model.stat.ColumnStat;
import org.apache.xtable.model.stat.PartitionValue;
import org.apache.xtable.model.stat.Range;
import org.apache.xtable.model.storage.*;
import org.apache.xtable.schema.SchemaFieldFinder;
import org.apache.xtable.spi.extractor.ConversionSource;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Builder
public class ParquetConversionSource implements ConversionSource<Long> {

  private final String tableName;
  private final String basePath;
  @NonNull private final Configuration hadoopConf;

  //    @Getter(value = AccessLevel.PACKAGE)
  //  private final FileStatus latestFileStatus = initFileStatus();

  @Builder.Default
  private static final AvroSchemaConverter schemaExtractor = AvroSchemaConverter.getInstance();

  private static <T> Stream<T> remoteIteratorToStream(RemoteIterator<T> remoteIterator) {
    Iterator<T> iterator =
        new Iterator<T>() {
          @Override
          public boolean hasNext() {
            try {
              return remoteIterator.hasNext();
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          public T next() {
            try {
              return remoteIterator.next();
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        };
    return StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false);
  }

  private Stream<LocatedFileStatus> initFileStatus() {
    try {
      FileSystem fs = FileSystem.get(hadoopConf);
      RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator =
          fs.listFiles(new Path(basePath), true);
      Stream<LocatedFileStatus> locatedFileStatusStream =
          remoteIteratorToStream(locatedFileStatusRemoteIterator)
              .filter(f -> f.getPath().getName().endsWith("parquet"));
      return locatedFileStatusStream;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public InternalTable getTable(Long aLong) {
    ParquetMetadata metadata = null;
    try {

      Optional<LocatedFileStatus> latestFile =
          initFileStatus().max(Comparator.comparing(LocatedFileStatus::getModificationTime));
      metadata = ParquetFileReader.readFooter(hadoopConf, latestFile.get().getPath());
      metadata.getBlocks().stream().mapToLong(b -> b.getRowCount()).sum();
      MessageType messageType = metadata.getFileMetaData().getSchema();
      org.apache.parquet.avro.AvroSchemaConverter avroSchemaConverter =
          new org.apache.parquet.avro.AvroSchemaConverter();
      Schema avroSchema = avroSchemaConverter.convert(messageType);
      InternalSchema schema = schemaExtractor.toInternalSchema(avroSchema);

      Set<String> partitionKeys = getPartitionFromDirectoryStructure(basePath).keySet();
      List<InternalPartitionField> partitionFields =
          partitionKeys.isEmpty()
              ? Collections.emptyList()
              : getInternalPartitionField(partitionKeys, schema);
      DataLayoutStrategy dataLayoutStrategy =
          partitionFields.isEmpty()
              ? DataLayoutStrategy.FLAT
              : DataLayoutStrategy.HIVE_STYLE_PARTITION;
      return InternalTable.builder()
          .tableFormat(TableFormat.PARQUET)
          .basePath(basePath)
          .name(tableName)
          .layoutStrategy(dataLayoutStrategy)
          .partitioningFields(partitionFields)
          .readSchema(schema)
          .latestCommitTime(Instant.ofEpochMilli(latestFile.get().getModificationTime()))
          .build();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private List<InternalPartitionField> getInternalPartitionField(
      Set<String> partitionList, InternalSchema schema) {
    List<InternalPartitionField> partitionFields = new ArrayList<>();

    for (String partitionKey : partitionList) {

      partitionFields.add(
          InternalPartitionField.builder()
              .sourceField(SchemaFieldFinder.getInstance().findFieldByPath(schema, partitionKey))
              .transformType(PartitionTransformType.VALUE)
              .build());
    }

    return partitionFields;
  }

  private Map<String, List<String>> getPartitionFromDirectoryStructure(String basePath) {

    try {
      FileSystem fs = FileSystem.get(hadoopConf);
      FileStatus[] baseFileStatus = fs.listStatus(new Path(basePath));
      Map<String, List<String>> partitionMap = new HashMap<>();

      for (FileStatus dirStatus : baseFileStatus) {
        if (dirStatus.isDirectory()) {
          String partitionPath = dirStatus.getPath().getName();
          if (partitionPath.contains("=")) {
            String[] partitionKeyValue = partitionPath.split("=");
            partitionMap
                .computeIfAbsent(partitionKeyValue[0], k -> new ArrayList<String>())
                .add(partitionKeyValue[1]);
          }
        }
      }

      System.out.println("Detected partition " + partitionMap);
      return partitionMap;

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public InternalSnapshot getCurrentSnapshot() {

    List<LocatedFileStatus> latestFile = initFileStatus().collect(Collectors.toList());
    InternalTable table = getTable(-1L);
    Map<String, List<String>> partitionKeys = getPartitionFromDirectoryStructure(basePath);
    List<InternalPartitionField> partitionFields =
        getInternalPartitionField(partitionKeys.keySet(), table.getReadSchema());

    List<InternalDataFile> internalDataFiles =
        latestFile.stream()
            .map(
                file -> {
                  System.out.println("Get file path " + file.getPath().toString());

                  return InternalDataFile.builder()
                      .physicalPath(file.getPath().toString())
                      .fileFormat(FileFormat.APACHE_PARQUET)
                      .fileSizeBytes(file.getLen())
                      .partitionValues(
                          getPartitionValue(file.getPath().toString(), table.getReadSchema()))
                      .lastModified(file.getModificationTime())
                      .columnStats(getColumnStatsForaFile(file, table))
                      .build();
                })
            .collect(Collectors.toList());

    return InternalSnapshot.builder()
        .table(table)
        .partitionedDataFiles(PartitionFileGroup.fromFiles(internalDataFiles))
        .build();
  }

  private List<PartitionValue> getPartitionValue(String filePath, InternalSchema schema) {
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

  @Override
  public TableChange getTableChangeForCommit(Long aLong) {
    try {
      FileSystem fs = FileSystem.get(hadoopConf);

      List<FileStatus> tableChanges =
          Arrays.stream(fs.listStatus(new Path(basePath)))
              .filter(FileStatus::isFile)
              .filter(fileStatus -> fileStatus.getModificationTime() > aLong)
              .collect(Collectors.toList());
      InternalTable internalTable = getTable(-1L);
      Set<InternalDataFile> internalDataFiles = new HashSet<>();
      System.out.println("Table changes " + tableChanges);
      for (FileStatus tableStatus : tableChanges) {
        System.out.println("Trying to get stats for " + tableStatus.getPath().toString());
        internalDataFiles.add(
            InternalDataFile.builder()
                .physicalPath(tableStatus.getPath().toString())
                .partitionValues(Collections.emptyList())
                .lastModified(tableStatus.getModificationTime())
                .fileSizeBytes(tableStatus.getLen())
                .columnStats(getColumnStatsForaFile(tableStatus, internalTable))
                .build());
      }

      return TableChange.builder()
          .tableAsOfChange(internalTable)
          .filesDiff(DataFilesDiff.builder().filesAdded(internalDataFiles).build())
          .build();

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public CommitsBacklog<Long> getCommitsBacklog(
      InstantsForIncrementalSync instantsForIncrementalSync) {
    System.out.println(
        "getCommitsBacklog is called even for full sync" + instantsForIncrementalSync);
    List<Long> commitsToProcess =
        Collections.singletonList(instantsForIncrementalSync.getLastSyncInstant().toEpochMilli());

    return CommitsBacklog.<Long>builder().commitsToProcess(commitsToProcess).build();
  }

  @Override
  public boolean isIncrementalSyncSafeFrom(Instant instant) {
    return true;
  }

  @Override
  public void close() throws IOException {}

  private List<ColumnStat> getColumnStatsForaFile(
      FileStatus fileStatus, InternalTable internalTable) {
    try {
      List<ColumnStat> columnStats = new ArrayList<>();
      ParquetMetadata parquetMetadata =
          ParquetFileReader.readFooter(hadoopConf, fileStatus.getPath());

      for (InternalField field : internalTable.getReadSchema().getAllFields()) {
        Optional<Statistics> columnStatistics =
            parquetMetadata.getBlocks().stream()
                .flatMap(block -> block.getColumns().stream())
                .filter(
                    column -> {
                       return column.getPath().toDotString().equals(field.getPath());
                    })
                .map(column -> column.getStatistics())
                .reduce(
                    (rg1, rg2) -> {
                  rg1.mergeStatistics(rg2);
                      System.out.println("rg1 " + rg1.genericGetMin());

                      return rg1;
                    });
        if (!columnStatistics.isPresent()) {
          System.out.println("Column stats null for " + field.getPath());
          System.out.println("");
        }
        if (columnStatistics.isPresent()) {
          System.out.println("Column stats PRESENT for " + field.getPath());
          System.out.println("Min value " + columnStatistics.get().genericGetMin());
          System.out.println("Min value String  " + columnStatistics.get().minAsString());

          columnStats.add(
              ColumnStat.builder()
                  .field(field)
                  .numNulls(columnStatistics.get().getNumNulls())
                  .range(
                      Range.vector(
                          columnStatistics.get().genericGetMin(),
                          columnStatistics.get().genericGetMax()))
                  .build());
        }
      }
      return columnStats;

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private List<ColumnStat> getColumnStatsForaFile(
      LocatedFileStatus fileStatus, InternalTable internalTable) {
    try {
      List<ColumnStat> columnStats = new ArrayList<>();
      ParquetMetadata parquetMetadata =
          ParquetFileReader.readFooter(hadoopConf, fileStatus.getPath());

      for (InternalField field : internalTable.getReadSchema().getAllFields()) {
        Optional<Statistics> columnStatistics =
            parquetMetadata.getBlocks().stream()
                .flatMap(block -> block.getColumns().stream())
                .filter(
                    column -> {
                      return column.getPath().toDotString().equals(field.getPath());
                    })
                .map(column -> column.getStatistics())
                .reduce(
                    (rg1, rg2) -> {
                      rg1.mergeStatistics(rg2);
                      return rg1;
                    });
        if (!columnStatistics.isPresent()) {
          System.out.println("Column stats null for " + field.getPath());
        }
        if (columnStatistics.isPresent()) {
          columnStats.add(
              ColumnStat.builder()
                  .field(field)
                  .numNulls(columnStatistics.get().getNumNulls())
                  .range(
                      Range.vector(
                          columnStatistics.get().minAsString(),
                          columnStatistics.get().maxAsString()))
                  .build());
        }
      }
      return columnStats;

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
