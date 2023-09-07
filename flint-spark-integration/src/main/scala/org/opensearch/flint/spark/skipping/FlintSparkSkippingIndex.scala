/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping

import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization
import org.opensearch.flint.core.metadata.FlintMetadata
import org.opensearch.flint.spark.{FlintSpark, FlintSparkIndex, FlintSparkIndexBuilder}
import org.opensearch.flint.spark.FlintSparkIndex.ID_COLUMN
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.{getSkippingIndexName, FILE_PATH_COLUMN, SKIPPING_INDEX_TYPE}
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.SkippingKindSerializer
import org.opensearch.flint.spark.skipping.minmax.MinMaxSkippingStrategy
import org.opensearch.flint.spark.skipping.partition.PartitionSkippingStrategy
import org.opensearch.flint.spark.skipping.valueset.ValueSetSkippingStrategy

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.dsl.expressions.DslExpression
import org.apache.spark.sql.flint.FlintDataSourceV2.FLINT_DATASOURCE
import org.apache.spark.sql.flint.datatype.FlintDataType
import org.apache.spark.sql.functions.{col, input_file_name, sha1}
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.types.StructType

/**
 * Flint skipping index in Spark.
 *
 * @param tableName
 *   source table name
 * @param indexedColumns
 *   indexed column list
 */
class FlintSparkSkippingIndex(
    tableName: String,
    val indexedColumns: Seq[FlintSparkSkippingStrategy])
    extends FlintSparkIndex {

  require(indexedColumns.nonEmpty, "indexed columns must not be empty")

  /** Required by json4s write function */
  implicit val formats: Formats = Serialization.formats(NoTypeHints) + SkippingKindSerializer

  /** Skipping index type */
  override val kind: String = SKIPPING_INDEX_TYPE

  override def name(): String = {
    getSkippingIndexName(tableName)
  }

  override def metadata(): FlintMetadata = {
    new FlintMetadata(s"""{
        |   "_meta": {
        |     "name": "${name()}",
        |     "kind": "$SKIPPING_INDEX_TYPE",
        |     "indexedColumns": $getMetaInfo,
        |     "source": "$tableName"
        |   },
        |   "properties": $getSchema
        | }
        |""".stripMargin)
  }

  override def buildBatch(flint: FlintSpark): DataFrameWriter[Row] = {
    build(flint.spark.read.table(tableName)).write
  }

  override def buildStream(flint: FlintSpark): DataStreamWriter[Row] = {
    flint.spark
      .readStream
      .table(tableName)
      .writeStream
      .foreachBatch { (batch: DataFrame, _: Long) =>
        build(batch)
          .withColumn(ID_COLUMN, sha1(col(FILE_PATH_COLUMN)))
          .write
          .format(FLINT_DATASOURCE)
          .options(flint.flintSparkConf.properties)
          .mode(SaveMode.Overwrite)
          .save(name())
      }
  }

  private def build(df: DataFrame): DataFrame = {
    val outputNames = indexedColumns.flatMap(_.outputSchema().keys)
    val aggFuncs = indexedColumns.flatMap(_.getAggregators)

    // Wrap aggregate function with output column name
    val namedAggFuncs =
      (outputNames, aggFuncs).zipped.map { case (name, aggFunc) =>
        new Column(aggFunc.toAggregateExpression().as(name))
      }

    df.groupBy(input_file_name().as(FILE_PATH_COLUMN))
      .agg(namedAggFuncs.head, namedAggFuncs.tail: _*)
  }

  private def getMetaInfo: String = {
    Serialization.write(indexedColumns)
  }

  private def getSchema: String = {
    val allFieldTypes =
      indexedColumns.flatMap(_.outputSchema()).toMap + (FILE_PATH_COLUMN -> "string")
    val catalogDDL =
      allFieldTypes
        .map { case (colName, colType) => s"$colName $colType not null" }
        .mkString(",")
    val allFieldSparkTypes = StructType.fromDDL(catalogDDL)
    // Convert StructType to {"properties": ...} and only need the properties value
    val properties = FlintDataType.serialize(allFieldSparkTypes)
    compact(render(parse(properties) \ "properties"))
  }
}

object FlintSparkSkippingIndex {

  /** Index type name */
  val SKIPPING_INDEX_TYPE = "skipping"

  /** File path column name */
  val FILE_PATH_COLUMN = "file_path"

  /**
   * Get skipping index name which follows the convention: "flint_" prefix + source table name +
   * "_skipping_index" suffix.
   *
   * This helps identify the Flint index because Flint index is not registered to Spark Catalog
   * for now.
   *
   * @param tableName
   *   full table name
   * @return
   *   Flint skipping index name
   */
  def getSkippingIndexName(tableName: String): String = {
    require(tableName.contains("."), "Full table name database.table is required")

    s"flint_${tableName.replace(".", "_")}_skipping_index"
  }

  /** Builder class for skipping index build */
  class Builder(flint: FlintSpark) extends FlintSparkIndexBuilder(flint) {
    private var indexedColumns: Seq[FlintSparkSkippingStrategy] = Seq()

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
     * Add partition skipping indexed columns.
     *
     * @param colNames
     *   indexed column names
     * @return
     *   index builder
     */
    def addPartitions(colNames: String*): Builder = {
      require(tableName.nonEmpty, "table name cannot be empty")

      colNames
        .map(findColumn)
        .map(col => PartitionSkippingStrategy(columnName = col.name, columnType = col.dataType))
        .foreach(addIndexedColumn)
      this
    }

    /**
     * Add value set skipping indexed column.
     *
     * @param colName
     *   indexed column name
     * @return
     *   index builder
     */
    def addValueSet(colName: String): Builder = {
      require(tableName.nonEmpty, "table name cannot be empty")

      val col = findColumn(colName)
      addIndexedColumn(ValueSetSkippingStrategy(columnName = col.name, columnType = col.dataType))
      this
    }

    /**
     * Add min max skipping indexed column.
     *
     * @param colName
     *   indexed column name
     * @return
     *   index builder
     */
    def addMinMax(colName: String): Builder = {
      val col = findColumn(colName)
      indexedColumns =
        indexedColumns :+ MinMaxSkippingStrategy(columnName = col.name, columnType = col.dataType)
      this
    }

    override def buildIndex(): FlintSparkIndex =
      new FlintSparkSkippingIndex(tableName, indexedColumns)

    private def addIndexedColumn(indexedCol: FlintSparkSkippingStrategy): Unit = {
      require(
        indexedColumns.forall(_.columnName != indexedCol.columnName),
        s"${indexedCol.columnName} is already indexed")

      indexedColumns = indexedColumns :+ indexedCol
    }
  }
}
