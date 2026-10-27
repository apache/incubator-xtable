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
 
package org.apache.xtable.spark.extensions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.Identifier

import org.apache.iceberg.{CatalogProperties, CatalogUtil, Table}
import org.apache.iceberg.types.Type.TypeID

import scala.collection.JavaConverters._

import org.apache.xtable.catalog.TableFormatUtils
import org.apache.xtable.conversion.SourceTable
import org.apache.xtable.iceberg.IcebergCatalogConfig
import org.apache.xtable.model.storage.TableFormat

/**
 * An XTable index on one column of an Iceberg table. A table has at most one XTable index, and its
 * column does not change: to index another column, drop the index and create a new one. The
 * definition is kept in the table properties, so every session and engine that reads the table can
 * find it:
 *
 *   - {@code xtable.index.<name>.column}: the indexed column
 *   - {@code xtable.index.<name>.option.<key>}: an option of the index, passed to the Hudi target
 *     that stores it, for example {@code xtable.hudi.target.metadata.record.index.min.filegroup.count}
 */
case class IndexDefinition(name: String, column: String, options: Map[String, String]) {
  def properties: Map[String, String] =
    Map(IndexDefinition.columnKey(name) -> column) ++ options.map { case (key, value) =>
      IndexDefinition.optionPrefix(name) + key -> value
    }
}

object IndexDefinition {
  val IndexType = "xtable"

  /**
   * Column types an index supports, which are the types whose filter literals the pruning rule
   * converts to Iceberg values.
   */
  val SupportedTypes: Set[TypeID] = Set(TypeID.STRING, TypeID.INTEGER, TypeID.LONG)

  private val Prefix = "xtable.index."
  private val ColumnSuffix = ".column"
  private val ValidName = "[A-Za-z0-9_]+".r

  def columnKey(name: String): String = Prefix + name + ColumnSuffix

  def optionPrefix(name: String): String = Prefix + name + ".option."

  def validateName(name: String): Unit = name match {
    case ValidName() =>
    case _ =>
      throw new IllegalArgumentException(
        s"Invalid index name '$name': only letters, digits and underscores are allowed")
  }

  /** Reads the index definitions from the properties of a table. */
  def fromProperties(properties: java.util.Map[String, String]): Seq[IndexDefinition] = {
    val props = properties.asScala
    props.keys.toSeq
      .filter(key => key.startsWith(Prefix) && key.endsWith(ColumnSuffix))
      .map(key => key.substring(Prefix.length, key.length - ColumnSuffix.length))
      .filter(name => ValidName.pattern.matcher(name).matches())
      .sorted
      .map { name =>
        val optionPrefix = IndexDefinition.optionPrefix(name)
        val options = props.collect {
          case (key, value) if key.startsWith(optionPrefix) =>
            key.substring(optionPrefix.length) -> value
        }.toMap
        IndexDefinition(name, props(columnKey(name)), options)
      }
  }

  /**
   * The XTable index of a table, configured with the column and options of its index definition.
   *
   * @param catalogName the Spark catalog the table was loaded from, if any
   * @param ident the identifier of the table in that catalog, if any
   */
  def newIndex(
      spark: SparkSession,
      table: Table,
      catalogName: Option[String],
      ident: Option[Identifier],
      definition: IndexDefinition): XTableIndex =
    XTableIndex(
      spark,
      sourceTable(
        table.name(),
        table.location(),
        TableFormatUtils
          .getTableDataLocation(TableFormat.ICEBERG, table.location(), table.properties()),
        catalogName,
        ident,
        spark.conf.getAll
      ),
      definition
    )

  /**
   * Describes how XTable loads the table for a sync. A table from a Spark catalog is loaded through
   * an Iceberg catalog built from the same {@code spark.sql.catalog.<name>.*} options, and a table
   * read by path is loaded from its location.
   *
   * @param dataPath the location of the data files of the table, where the Hudi metadata that holds
   *     the index is kept
   * @param sparkConf the Spark session configuration, which holds the catalog options
   */
  def sourceTable(
      tableName: String,
      tableLocation: String,
      dataPath: String,
      catalogName: Option[String],
      ident: Option[Identifier],
      sparkConf: Map[String, String]): SourceTable = {
    val builder = SourceTable
      .builder()
      .name(tableName)
      .basePath(tableLocation)
      .dataPath(dataPath)
      .formatName(TableFormat.ICEBERG)
    for (catalog <- catalogName; identifier <- ident) {
      val prefix = s"spark.sql.catalog.$catalog."
      val options = sparkConf.collect {
        case (key, value) if key.startsWith(prefix) => key.substring(prefix.length) -> value
      }
      builder
        .name(identifier.name())
        .namespace(identifier.namespace())
        .catalogConfig(
          IcebergCatalogConfig
            .builder()
            .catalogName(catalog)
            .catalogImpl(catalogImpl(options))
            .catalogOptions(options.asJava)
            .build())
    }
    builder.build()
  }

  /** Resolves the catalog implementation from the options the way Iceberg's Spark catalog does. */
  private def catalogImpl(options: Map[String, String]): String =
    options.getOrElse(
      CatalogProperties.CATALOG_IMPL,
      options
        .getOrElse(CatalogUtil.ICEBERG_CATALOG_TYPE, CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE)
        .toLowerCase match {
        case CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE => CatalogUtil.ICEBERG_CATALOG_HIVE
        case CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP => CatalogUtil.ICEBERG_CATALOG_HADOOP
        case CatalogUtil.ICEBERG_CATALOG_TYPE_REST => CatalogUtil.ICEBERG_CATALOG_REST
        case CatalogUtil.ICEBERG_CATALOG_TYPE_GLUE => CatalogUtil.ICEBERG_CATALOG_GLUE
        case CatalogUtil.ICEBERG_CATALOG_TYPE_NESSIE => CatalogUtil.ICEBERG_CATALOG_NESSIE
        case CatalogUtil.ICEBERG_CATALOG_TYPE_JDBC => CatalogUtil.ICEBERG_CATALOG_JDBC
        case other =>
          throw new IllegalArgumentException(s"Unknown Iceberg catalog type $other")
      }
    )
}
