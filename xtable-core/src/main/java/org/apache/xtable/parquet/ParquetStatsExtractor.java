
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HiddenFileFilter;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.statistics.Statistics;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.*;
// TODO add other methods of stats (row group vs columns)
public class ParquetStatsExtractor {
    @Builder.Default
    private static final ParquetMetadataExtractor parquetMetadataExtractor =
            ParquetMetadataExtractor.getInstance();

    private static Map<ColumnDescriptor, ColStats> stats = new LinkedHashMap<ColumnDescriptor, ColStats>();
    private static long recordCount = 0;
    private InternalDataFile toInternalDataFile(Configuration hadoopConf,
            String parentPath, Map<ColumnDescriptor, ColStats> stats) {
        FileSystem fs = FileSystem.get(hadoopConf);
        FileStatus file = fs.getFileStatus(new Path(parentPath));

        return InternalDataFile.builder()
                .physicalPath(parentPath)
                .fileFormat(FileFormat.APACHE_PARQUET)
                //TODO create parquetPartitionHelper Class getPartitionValue(
                //                                basePath,
                //                                file.getPath().toString(),
                //                                table.getReadSchema(),
                //                                partitionInfo))
                .partitionValues(/*schema.getDoc()*/)
                .fileSizeBytes(file.getLen())
                .recordCount(recordCount)
                .columnStats(stats.values().stream().collect(Collectors.toList()))
                .lastModified(file.getModificationTime())
                .build();
    }
    private static void getColumnStatsForaFile(ParquetMetadata footer) {
        for (BlockMetaData blockMetaData : footer.getBlocks()) {

            MessageType schema = parquetMetadataExtractor.getSchema(footer)
            recordCount += blockMetaData.getRowCount();
            List<ColumnChunkMetaData> columns = blockMetaData.getColumns();
            for (ColumnChunkMetaData columnMetaData : columns) {
                ColumnDescriptor desc =
                        schema.getColumnDescription(columnMetaData.getPath().toArray());
                ColStats.add(
                        desc,
                        columnMetaData.getValueCount(),
                        columnMetaData.getTotalSize(),
                        columnMetaData.getTotalUncompressedSize(),
                        columnMetaData.getEncodings(),
                        columnMetaData.getStatistics());
            }
        }
    }
    private static class Stats {
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        long total = 0;

        public void add(long length) {
            min = Math.min(length, min);
            max = Math.max(length, max);
            total += length;
        }
    }
    private static class ColStats {

        Stats valueCountStats = new Stats();
        Stats allStats = new Stats();
        Stats uncStats = new Stats();
        Set<Encoding> encodings = new TreeSet<Encoding>();
        Statistics colValuesStats = null;
        int blocks = 0;

        public void add(
                long valueCount, long size, long uncSize, Collection<Encoding> encodings, Statistics colValuesStats) {
            ++blocks;
            valueCountStats.add(valueCount);
            allStats.add(size);
            uncStats.add(uncSize);
            this.encodings.addAll(encodings);
            this.colValuesStats = colValuesStats;
        }
        private static void add(
                ColumnDescriptor desc,
                long valueCount,
                long size,
                long uncSize,
                Collection<Encoding> encodings,
                Statistics colValuesStats) {
            ColStats colStats = stats.get(desc);
            if (colStats == null) {
                colStats = new ColStats();
                stats.put(desc, colStats);
            }
            colStats.add(valueCount, size, uncSize, encodings, colValuesStats);
        }
    }
}