/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.covering

import org.json4s.{Formats, NoTypeHints}
import org.json4s.JsonAST.{JArray, JObject, JString}
import org.json4s.native.JsonMethods.{compact, parse, render}
import org.json4s.native.Serialization
import org.opensearch.flint.core.metadata.FlintMetadata
import org.opensearch.flint.spark.{FlintSpark, FlintSparkIndex, FlintSparkIndexBuilder}
import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex.COVERING_INDEX_TYPE

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.flint.datatype.FlintDataType
import org.apache.spark.sql.types.StructType

/**
 * Flint covering index in Spark.
 *
 * @param name
 *   index name
 * @param tableName
 *   source table name
 * @param indexedColumns
 *   indexed column list
 */
class FlintSparkCoveringIndex(
    override val name: String,
    val tableName: String,
    val indexedColumns: Map[String, String])
    extends FlintSparkIndex {

  require(indexedColumns.nonEmpty, "indexed columns must not be empty")

  /** Required by json4s write function */
  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  override val kind: String = COVERING_INDEX_TYPE

  override def metadata(): FlintMetadata = {
    new FlintMetadata(s"""{
        |   "_meta": {
        |     "name": "$name",
        |     "kind": "$kind",
        |     "indexedColumns": $getMetaInfo,
        |     "source": "$tableName"
        |   },
        |   "properties": $getSchema
        | }
        |""".stripMargin)
  }

  override def build(df: DataFrame): DataFrame = {
    val colNames = indexedColumns.keys.toSeq
    df.select(colNames.head, colNames.tail: _*)
  }

  private def getMetaInfo: String = {
    val objects = indexedColumns.map { case (colName, colType) =>
      JObject("columnName" -> JString(colName), "columnType" -> JString(colType))
    }.toList
    Serialization.write(JArray(objects))
  }

  private def getSchema: String = {
    val catalogDDL =
      indexedColumns
        .map { case (colName, colType) => s"$colName $colType not null" }
        .mkString(",")
    val properties = FlintDataType.serialize(StructType.fromDDL(catalogDDL))
    compact(render(parse(properties) \ "properties"))
  }
}

object FlintSparkCoveringIndex {

  /** Covering index type name */
  val COVERING_INDEX_TYPE = "covering"

  /** Builder class for covering index build */
  class Builder(flint: FlintSpark) extends FlintSparkIndexBuilder(flint) {
    private var indexName: String = ""
    private var indexedColumns: Map[String, String] = Map()

    /**
     * Set covering index name.
     *
     * @param indexName
     *   index name
     * @return
     *   index builder
     */
    def indexName(indexName: String): Builder = {
      this.indexName = indexName
      this
    }

    /**
     * Configure which source table the index is based on.
     *
     * @param tableName
     *   full table name
     * @return
     *   index builder
     */
    def onTable(tableName: String): Builder = {
      this.tableName = tableName
      this
    }

    /**
     * Add indexed column name.
     *
     * @param colNames
     *   column names
     * @return
     *   index builder
     */
    def addIndexColumn(colNames: String*): Builder = {
      colNames.foreach(colName => {
        indexedColumns += (colName -> findColumn(colName).dataType)
      })
      this
    }

    override protected def buildIndex(): FlintSparkIndex =
      new FlintSparkCoveringIndex(indexName, tableName, indexedColumns)
  }
}