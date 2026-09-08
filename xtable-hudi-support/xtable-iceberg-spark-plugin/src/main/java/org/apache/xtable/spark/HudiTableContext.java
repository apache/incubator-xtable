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
 
package org.apache.xtable.spark;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import lombok.Getter;
import lombok.ToString;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.connector.catalog.Identifier;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.keygen.ComplexKeyGenerator;
import org.apache.hudi.keygen.NonpartitionedKeyGenerator;
import org.apache.hudi.keygen.SimpleKeyGenerator;

import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.types.Types;

import org.apache.xtable.IcebergFormatConfig;
import org.apache.xtable.model.storage.TableFormat;

/**
 * Everything the Hudi write path needs about one Hudi-managed Iceberg table, derived from the
 * Iceberg table metadata and the Spark catalog it was loaded from. Nothing here is typed by the
 * user: the record key comes from the Iceberg identifier fields, the partition path from identity
 * partition transforms, the base path from the table's data location.
 */
@Getter
@ToString
public class HudiTableContext implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String catalogName;
  private final String catalogImpl;
  private final Map<String, String> catalogOptions;
  private final String[] namespace;
  private final String tableName;
  private final String basePath;
  private final List<String> recordKeyFields;
  private final List<String> partitionFields;
  private final String orderingField;
  private final boolean mergeOnRead;
  private final int formatVersion;
  private final Map<String, String> writeOverrides;
  /** Non-null when the table shape cannot be handled by Hudi; writes then stay on Iceberg. */
  private final String unsupportedReason;

  private HudiTableContext(
      String catalogName,
      String catalogImpl,
      Map<String, String> catalogOptions,
      String[] namespace,
      String tableName,
      String basePath,
      List<String> recordKeyFields,
      List<String> partitionFields,
      String orderingField,
      boolean mergeOnRead,
      int formatVersion,
      Map<String, String> writeOverrides,
      String unsupportedReason) {
    this.catalogName = catalogName;
    this.catalogImpl = catalogImpl;
    this.catalogOptions = catalogOptions;
    this.namespace = namespace;
    this.tableName = tableName;
    this.basePath = basePath;
    this.recordKeyFields = recordKeyFields;
    this.partitionFields = partitionFields;
    this.orderingField = orderingField;
    this.mergeOnRead = mergeOnRead;
    this.formatVersion = formatVersion;
    this.writeOverrides = writeOverrides;
    this.unsupportedReason = unsupportedReason;
  }

  public static HudiTableContext from(
      Identifier identifier,
      Table table,
      String catalogName,
      String catalogImpl,
      Map<String, String> catalogOptions,
      Configuration hadoopConf) {
    Map<String, String> properties = table.properties();
    String basePath = properties.get(TableProperties.WRITE_DATA_LOCATION);
    if (basePath == null || basePath.isEmpty()) {
      basePath = stripTrailingSlash(table.location()) + "/data";
    }
    basePath = qualify(basePath, hadoopConf);
    // Iceberg identifier fields are a set; Hudi record key order matters, so follow the schema
    Set<String> identifierFields = table.schema().identifierFieldNames();
    List<String> recordKeys = new ArrayList<>();
    for (Types.NestedField column : table.schema().columns()) {
      if (identifierFields.contains(column.name())) {
        recordKeys.add(column.name());
      }
    }
    List<String> partitionFields = new ArrayList<>();
    String unsupported = null;
    for (PartitionField field : table.spec().fields()) {
      if (field.transform().isIdentity()) {
        partitionFields.add(table.schema().findColumnName(field.sourceId()));
      } else if (!field.transform().isVoid()) {
        unsupported =
            String.format(
                "partition transform %s(%s) has no Hudi equivalent yet",
                field.transform(), table.schema().findColumnName(field.sourceId()));
      }
    }
    boolean mergeOnRead = isMergeOnRead(properties);
    int formatVersion = formatVersion(table);
    if (mergeOnRead && formatVersion < 3 && unsupported == null) {
      unsupported =
          "merge-on-read needs an Iceberg format-version 3 table for deletion vectors; "
              + "set TBLPROPERTIES ('format-version'='3')";
    }
    Map<String, String> overrides = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      if (entry.getKey().startsWith(HudiIcebergConf.TABLE_PROP_WRITE_PREFIX)) {
        overrides.put(
            "hoodie." + entry.getKey().substring(HudiIcebergConf.TABLE_PROP_WRITE_PREFIX.length()),
            entry.getValue());
      }
    }
    return new HudiTableContext(
        catalogName,
        catalogImpl,
        catalogOptions == null ? Collections.emptyMap() : new HashMap<>(catalogOptions),
        identifier.namespace(),
        identifier.name(),
        basePath,
        Collections.unmodifiableList(recordKeys),
        Collections.unmodifiableList(partitionFields),
        properties.get(HudiIcebergConf.TABLE_PROP_ORDERING_FIELD),
        mergeOnRead,
        formatVersion,
        overrides,
        unsupported);
  }

  /**
   * Iceberg's own row-level write modes decide the Hudi table type: any of {@code
   * write.merge.mode}, {@code write.update.mode}, {@code write.delete.mode} set to {@code
   * merge-on-read} means a Hudi merge-on-read table whose updates land as deletion vectors. {@code
   * hudi.table-type} overrides.
   */
  static boolean isMergeOnRead(Map<String, String> properties) {
    String explicit = properties.get(HudiIcebergConf.TABLE_PROP_TABLE_TYPE);
    if (explicit != null) {
      return explicit.toLowerCase(Locale.ROOT).startsWith("m");
    }
    for (String key :
        new String[] {
          TableProperties.MERGE_MODE, TableProperties.UPDATE_MODE, TableProperties.DELETE_MODE
        }) {
      if ("merge-on-read".equalsIgnoreCase(properties.get(key))) {
        return true;
      }
    }
    return false;
  }

  static int formatVersion(Table table) {
    if (table instanceof HasTableOperations) {
      return ((HasTableOperations) table).operations().current().formatVersion();
    }
    return Integer.parseInt(table.properties().getOrDefault("format-version", "2"));
  }

  public HoodieTableType tableType() {
    return mergeOnRead ? HoodieTableType.MERGE_ON_READ : HoodieTableType.COPY_ON_WRITE;
  }

  public boolean isKeyed() {
    return !recordKeyFields.isEmpty();
  }

  public String databaseName() {
    return namespace == null || namespace.length == 0 ? null : String.join(".", namespace);
  }

  public String keyGeneratorClass() {
    if (partitionFields.isEmpty()) {
      return NonpartitionedKeyGenerator.class.getName();
    }
    if (recordKeyFields.size() <= 1 && partitionFields.size() <= 1) {
      return SimpleKeyGenerator.class.getName();
    }
    return ComplexKeyGenerator.class.getName();
  }

  public IcebergFormatConfig formatConfig() {
    return IcebergFormatConfig.of(catalogName, catalogImpl, catalogOptions, namespace, false);
  }

  /** The Hudi datasource options for one write of the given kind. */
  public Map<String, String> writeParams(
      HudiWriteOperation operation, Map<String, String> sessionOverrides) {
    Map<String, String> params = new LinkedHashMap<>();
    params.put("path", basePath);
    params.put("hoodie.table.name", tableName);
    if (databaseName() != null) {
      params.put("hoodie.database.name", databaseName());
    }
    params.put("hoodie.datasource.write.table.type", tableType().name());
    if (mergeOnRead) {
      // Each update becomes a positional delete plus an insert, which is exactly what an Iceberg
      // V3 deletion vector plus a new data file expresses
      params.put("hoodie.write.updates.as.deletes.and.inserts", "true");
      params.put("hoodie.write.record.positions", "true");
      params.put("hoodie.index.type", "SIMPLE");
    }
    params.put("hoodie.table.format", TableFormat.ICEBERG);
    params.put("hoodie.metadata.enable", "false");
    params.put("hoodie.datasource.meta.sync.enable", "false");
    params.put("hoodie.datasource.write.hive_style_partitioning", "true");
    params.put("hoodie.datasource.write.partitionpath.urlencode", "false");
    params.put("hoodie.datasource.write.partitionpath.field", String.join(",", partitionFields));
    if (isKeyed()) {
      params.put("hoodie.datasource.write.recordkey.field", String.join(",", recordKeyFields));
      params.put("hoodie.datasource.write.keygenerator.class", keyGeneratorClass());
    }
    if (mergeOnRead) {
      // A positional delete plus insert has no notion of a newer or older version of a record:
      // the latest write wins, which is commit-time ordering. An ordering field is ignored.
      params.put("hoodie.record.merge.mode", "COMMIT_TIME_ORDERING");
    } else if (orderingField != null) {
      params.put("hoodie.datasource.write.precombine.field", orderingField);
    }
    if (operation.hoodieOperation() != null) {
      params.put("hoodie.datasource.write.operation", operation.hoodieOperation());
    } else if (isKeyed()) {
      params.put("hoodie.datasource.write.operation", "upsert");
    }
    params.putAll(writeOverrides);
    if (sessionOverrides != null) {
      params.putAll(sessionOverrides);
    }
    return params;
  }

  /**
   * Hudi keeps the base path as given and builds file paths from it, while its file-system view
   * lists fully qualified paths. A scheme-less base path would make the two disagree, and Iceberg
   * matches files by exact path string, so qualify it once here.
   */
  private static String qualify(String basePath, Configuration hadoopConf) {
    if (hadoopConf == null) {
      return basePath;
    }
    try {
      Path path = new Path(basePath);
      return path.getFileSystem(hadoopConf).makeQualified(path).toString();
    } catch (IOException | RuntimeException e) {
      return basePath;
    }
  }

  private static String stripTrailingSlash(String path) {
    return path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
  }
}
