package org.apache.xtable.catalog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.xtable.CatalogTableBuilder;
import org.apache.xtable.catalog.hms.HMSSchemaExtractor;
import org.apache.xtable.catalog.hms.IcebergHMSCatalogSyncRequestProvider;
import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.model.storage.TableFormat;

public class CatalogTableBuilderFactory {
  public static CatalogTableBuilder<Table> getHmsCatalogTableBuilder(
      String tableFormat, Configuration configuration, HMSSchemaExtractor schemaExtractor) {
    switch (tableFormat) {
      case TableFormat.ICEBERG:
        return new IcebergHMSCatalogSyncRequestProvider(configuration, schemaExtractor);
      default:
        throw new NotSupportedException("Unsupported table format: " + tableFormat);
    }
  }
}
