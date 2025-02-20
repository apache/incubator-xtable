
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HiddenFileFilter;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.MessageType;

// TODO add other methods of stats (row group vs columns)
public class ParquetTableExtractor {
    private static void add(ParquetMetadata footer) {
        for (BlockMetaData blockMetaData : footer.getBlocks()) {

            MessageType schema = footer.getFileMetaData().getSchema();
            recordCount += blockMetaData.getRowCount();
            List<ColumnChunkMetaData> columns = blockMetaData.getColumns();
            for (ColumnChunkMetaData columnMetaData : columns) {
                ColumnDescriptor desc =
                        schema.getColumnDescription(columnMetaData.getPath().toArray());
                add(
                        desc,
                        columnMetaData.getValueCount(),
                        columnMetaData.getTotalSize(),
                        columnMetaData.getTotalUncompressedSize(),
                        columnMetaData.getEncodings(),
                        columnMetaData.getStatistics());
            }
        }
    }
}