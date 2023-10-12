/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.opensearch.flint.core.metadata.FlintMetadata
import org.opensearch.flint.spark.FlintSparkIndex.BatchRefresh

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.flint.datatype.FlintDataType
import org.apache.spark.sql.types.StructType

/**
 * Flint index interface in Spark.
 */
trait FlintSparkIndex extends BatchRefresh {

  /**
   * Index type
   */
  val kind: String

  /**
   * Index options
   */
  val options: FlintSparkIndexOptions

  /**
   * @return
   *   Flint index name
   */
  def name(): String

  /**
   * @return
   *   Flint index metadata
   */
  def metadata(): FlintMetadata

  /**
   * Build a data frame to represent index data computation logic. Upper level code decides how to
   * use this, ex. batch or streaming, fully or incremental refresh.
   *
   * @param df
   *   data frame to append building logic
   * @return
   *   index building data frame
   */
  /*
  def build(df: DataFrame): DataFrame

  def buildBatch(spark: SparkSession): DataFrameWriter[Row]

  def buildStream(spark: SparkSession): DataStreamWriter[Row]
   */
}

object FlintSparkIndex {

  trait BatchRefresh {

    def build(spark: SparkSession, df: Option[DataFrame]): DataFrame
  }

  trait StreamingRefresh {

    def build(spark: SparkSession): DataFrame
  }

  /**
   * ID column name.
   */
  val ID_COLUMN: String = "__id__"

  /**
   * Common prefix of Flint index name which is "flint_database_table_"
   *
   * @param fullTableName
   *   source full table name
   * @return
   *   Flint index name
   */
  def flintIndexNamePrefix(fullTableName: String): String =
    s"flint_${fullTableName.replace(".", "_")}_"

  /**
   * Populate environment variables to persist in Flint metadata.
   *
   * @return
   *   env key value mapping to populate
   */
  def populateEnvToMetadata: Map[String, String] = {
    // TODO: avoid hardcoding env name below by providing another config
    val envNames = Seq("SERVERLESS_EMR_VIRTUAL_CLUSTER_ID", "SERVERLESS_EMR_JOB_ID")
    envNames
      .flatMap(key =>
        Option(System.getenv(key))
          .map(value => key -> value))
      .toMap
  }

  /**
   * Create Flint metadata builder with common fields.
   *
   * @param index
   *   Flint index
   * @return
   *   Flint metadata builder
   */
  def metadataBuilder(index: FlintSparkIndex): FlintMetadata.Builder = {
    val builder = new FlintMetadata.Builder()
    // Common fields
    builder.kind(index.kind)
    builder.options(index.options.optionsWithDefault.mapValues(_.asInstanceOf[AnyRef]).asJava)

    // Index properties
    val envs = populateEnvToMetadata
    if (envs.nonEmpty) {
      builder.addProperty("env", envs.asJava)
    }

    // Optional index settings
    val settings = index.options.indexSettings()
    if (settings.isDefined) {
      builder.indexSettings(settings.get)
    }
    builder
  }

  def generateSchemaJSON(allFieldTypes: Map[String, String]): String = {
    val catalogDDL =
      allFieldTypes
        .map { case (colName, colType) => s"$colName $colType not null" }
        .mkString(",")

    val structType = StructType.fromDDL(catalogDDL)
    FlintDataType.serialize(structType)
  }
}
