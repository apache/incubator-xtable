import org.apache.parquet.schema.MessageType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.parquet.Schema;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

public class ParquetMetadataExtractor {

    private static MessageType getSchema(ParquetMetadata footer) {
        MessageType schema = footer.getFileMetaData().getSchema();
        return schema
    }

    private static ParquetMetadata readParquetMetadata(HadoopConf conf, BasePath path) {
        ParquetMetadata footer =
                ParquetFileReader.readFooter(conf, path, ParquetMetadataConverter.NO_FILTER);
        return footer

    }
}