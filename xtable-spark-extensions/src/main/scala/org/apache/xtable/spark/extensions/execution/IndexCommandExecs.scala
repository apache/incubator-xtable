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
 
package org.apache.xtable.spark.extensions.execution

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{IndexAlreadyExistsException, NoSuchIndexException}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog, TableChange}
import org.apache.spark.sql.execution.datasources.v2.LeafV2CommandExec

import org.apache.iceberg.spark.source.SparkTable
import org.apache.iceberg.types.Type.TypeID

import scala.collection.JavaConverters._

import org.apache.xtable.spark.extensions.IndexDefinition

/**
 * Builds an XTable index on one column and records its definition in the table properties. The
 * properties are written only after the index is built, so a failed build leaves no definition. A
 * table has at most one XTable index.
 */
case class CreateIndexExec(
    catalog: TableCatalog,
    ident: Identifier,
    table: SparkTable,
    indexName: String,
    columns: Seq[Seq[String]],
    ignoreIfExists: Boolean,
    options: Map[String, String])
    extends LeafV2CommandExec {

  override def output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    IndexDefinition.validateName(indexName)
    val definitions = IndexDefinition.fromProperties(table.properties())
    if (definitions.exists(_.name.equalsIgnoreCase(indexName))) {
      if (ignoreIfExists) {
        return Nil
      }
      throw new IndexAlreadyExistsException(indexName, ident.toString, None)
    }
    definitions.headOption.foreach { existing =>
      throw new IllegalArgumentException(
        s"Table $ident already has the XTable index ${existing.name} on column " +
          s"${existing.column}. A table has one XTable index, so drop it to index another column")
    }
    val column = columns match {
      case Seq(Seq(name)) => name
      case _ =>
        throw new IllegalArgumentException(
          s"An XTable index covers exactly one top level column, but index $indexName has " +
            columns.map(_.mkString(".")).mkString("(", ", ", ")"))
    }
    val icebergTable = table.table()
    val field = Option(icebergTable.schema().caseInsensitiveFindField(column)).getOrElse(
      throw new IllegalArgumentException(s"Column $column does not exist in table $ident"))
    if (!IndexDefinition.SupportedTypes.contains(field.`type`().typeId())) {
      throw new IllegalArgumentException(
        s"Cannot index column ${field.name()} of type ${field.`type`()}: an XTable index supports " +
          IndexDefinition.SupportedTypes.map(_.toString.toLowerCase).mkString(", ") + " columns")
    }

    val definition = IndexDefinition(indexName, field.name(), options)
    IndexDefinition
      .newIndex(session, icebergTable, Some(catalog.name()), Some(ident), definition)
      .sync()
    catalog.alterTable(
      ident,
      definition.properties.map { case (key, value) =>
        TableChange.setProperty(key, value)
      }.toSeq: _*)
    Nil
  }
}

/** Drops an XTable index and removes its definition from the table properties. */
case class DropIndexExec(
    catalog: TableCatalog,
    ident: Identifier,
    table: SparkTable,
    indexName: String,
    ignoreIfNotExists: Boolean)
    extends LeafV2CommandExec {

  override def output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    val definitions = IndexDefinition.fromProperties(table.properties())
    definitions.find(_.name.equalsIgnoreCase(indexName)) match {
      case None if ignoreIfNotExists => Nil
      case None => throw new NoSuchIndexException(indexName, ident.toString, None)
      case Some(definition) =>
        val icebergTable = table.table()
        // drops what a failed build left behind too
        IndexDefinition
          .newIndex(session, icebergTable, Some(catalog.name()), Some(ident), definition)
          .drop()
        catalog.alterTable(
          ident,
          definition.properties.keys.map(key => TableChange.removeProperty(key)).toSeq: _*)
        Nil
    }
  }
}

/** Syncs an XTable index with the current snapshot of the table. */
case class RefreshIndexExec(
    catalog: TableCatalog,
    ident: Identifier,
    table: SparkTable,
    indexName: String)
    extends LeafV2CommandExec {

  override def output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    val definition = IndexDefinition
      .fromProperties(table.properties())
      .find(_.name.equalsIgnoreCase(indexName))
      .getOrElse(throw new NoSuchIndexException(indexName, ident.toString, None))
    val icebergTable = table.table()
    // the snapshot the table had when the command was planned may be stale
    icebergTable.refresh()
    IndexDefinition
      .newIndex(session, icebergTable, Some(catalog.name()), Some(ident), definition)
      .sync()
    Nil
  }
}
