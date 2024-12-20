package org.apache.xtable;

import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.catalog.CatalogTableIdentifier;

public interface CatalogTableBuilder<TABLE> {
  public TABLE getCreateTableRequest(InternalTable table, CatalogTableIdentifier tableIdentifier);

  public TABLE getUpdateTableRequest(InternalTable table, TABLE catalogTable, CatalogTableIdentifier tableIdentifier);
}
