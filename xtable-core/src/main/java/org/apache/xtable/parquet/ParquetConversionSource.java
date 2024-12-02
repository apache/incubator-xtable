package org.apache.xtable.parquet;

import lombok.Builder;
import lombok.NonNull;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.xtable.avro.AvroSchemaConverter;

import org.apache.xtable.model.*;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.stat.ColumnStat;
import org.apache.xtable.model.stat.Range;
import org.apache.xtable.model.storage.DataFilesDiff;
import org.apache.xtable.model.storage.DataLayoutStrategy;
import org.apache.xtable.model.storage.InternalDataFile;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.spi.extractor.ConversionSource;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Builder
public class ParquetConversionSource implements ConversionSource<Long> {

    private final String tableName;
    private final String basePath;
    @NonNull
    private final Configuration hadoopConf;

//    @Getter(value = AccessLevel.PACKAGE)
  //  private final FileStatus latestFileStatus = initFileStatus();

    @Builder.Default
    private static final AvroSchemaConverter schemaExtractor = AvroSchemaConverter.getInstance();

    private FileStatus initFileStatus() {
        try {
            FileSystem fs = FileSystem.get(hadoopConf);
            Optional<FileStatus> latestFile =Arrays.stream(fs.listStatus(new Path(basePath))).
             filter(FileStatus::isFile)
            .max(Comparator.comparing(FileStatus::getModificationTime));
            System.out.println("Latest file "+latestFile.get());
            return latestFile.get();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public InternalTable getTable(Long aLong) {
        ParquetMetadata metadata = null;
        try {
            FileSystem fs = FileSystem.get(hadoopConf);
            Optional<FileStatus> latestFile =Arrays.stream(fs.listStatus(new Path(basePath))).
                    filter(FileStatus::isFile)
                    .max(Comparator.comparing(FileStatus::getModificationTime));
            Path p = latestFile.get().getPath();
            metadata = ParquetFileReader.readFooter(hadoopConf,p);
            MessageType messageType = metadata.getFileMetaData().getSchema();
            org.apache.parquet.avro.AvroSchemaConverter avroSchemaConverter = new org.apache.parquet.avro.AvroSchemaConverter();
            Schema avroSchema = avroSchemaConverter.convert(messageType);
            InternalSchema schema = schemaExtractor.toInternalSchema(avroSchema);
            List<InternalPartitionField> partitionFields = Collections.emptyList();
            DataLayoutStrategy dataLayoutStrategy = DataLayoutStrategy.FLAT;
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

    @Override
    public InternalSnapshot getCurrentSnapshot() {
        InternalTable table = getTable(-1L);

        return InternalSnapshot.builder()
                .table(table)
                .partitionedDataFiles(Collections.emptyList())
                .build();
    }

    @Override
    public TableChange getTableChangeForCommit(Long aLong) {
        try {
            FileSystem fs = FileSystem.get(hadoopConf);
          List<FileStatus> tableChanges =Arrays.stream(fs.listStatus(new Path(basePath))).
                  filter(FileStatus::isFile)
            .filter(fileStatus -> fileStatus.getModificationTime() > aLong)
                  .collect(Collectors.toList());
          InternalTable internalTable = getTable(-1L);
          Set<InternalDataFile> internalDataFiles = new HashSet<>();

          for(FileStatus tableStatus : tableChanges)
          {
              internalDataFiles.add(
                      InternalDataFile.builder()
                              .physicalPath(tableStatus.getPath().toString())
                              .partitionValues(Collections.emptyList())
                              .lastModified(tableStatus.getModificationTime())
                              .fileSizeBytes(tableStatus.getLen())
                              .columnStats(getColumnStatsForaFile(tableStatus,internalTable))
                              .build()
              );

          }

            return TableChange.builder().tableAsOfChange(internalTable)
                    .filesDiff(DataFilesDiff.builder().filesAdded(internalDataFiles).build())
            .build();


        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }

    @Override
    public CommitsBacklog<Long> getCommitsBacklog(InstantsForIncrementalSync instantsForIncrementalSync) {
        System.out.println("getCommitsBacklog is called even for full sync"+instantsForIncrementalSync);
        List<Long> commitsToProcess = Collections.singletonList(instantsForIncrementalSync.getLastSyncInstant().toEpochMilli());

        return CommitsBacklog.<Long>builder().commitsToProcess(commitsToProcess).build();
    }

    @Override
    public boolean isIncrementalSyncSafeFrom(Instant instant) {
        return true;
    }

    @Override
    public void close() throws IOException {

    }

    private List<ColumnStat> getColumnStatsForaFile(FileStatus fileStatus, InternalTable internalTable)
    {
        try {
            List<ColumnStat> columnStats = new ArrayList<>();
            ParquetMetadata parquetMetadata =ParquetFileReader.readFooter(hadoopConf,fileStatus.getPath());

            for(InternalField field : internalTable.getReadSchema().getAllFields())
            {
                Optional<Statistics> columnStatistics = parquetMetadata.getBlocks().stream()
                                .flatMap(block -> block.getColumns().stream())
                                        .filter(column ->
                                                {
                                                    System.out.println("column "+column.getPath().toString());
                                                    System.out.println("column "+column.getPath().toDotString());
                                                    System.out.println("field.getPath() "+field.getPath());

                                                    return column.getPath().toDotString().equals(field.getPath());
                                                } )
                                                .map(column -> column.getStatistics())
                                                        .reduce((rg1,rg2) -> {
                                                            System.out.println("rg1 "+rg1.genericGetMin());
                                                            System.out.println("rg2 "+rg2.genericGetMin());

                                                            rg1.mergeStatistics(rg2);
                                                            System.out.println("rg1 "+rg1.genericGetMin());

                                                            return rg1;
                                                        });
                if(!columnStatistics.isPresent())
                {
                    System.out.println("Column stats null for "+field.getPath());
                    System.out.println("");
                }
                columnStats.add(
                        ColumnStat.builder()
                                .field(field)
                                .numNulls(columnStatistics.get().getNumNulls())
                                .range(Range.vector(columnStatistics.get().genericGetMin(),columnStatistics.get().genericGetMax()))
                                .build()
                )    ;
            }
System.out.println("sijze of column stats "+columnStats.get(0).getField()+" "+columnStats.get(0).getRange());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return Collections.emptyList();
    }
}
