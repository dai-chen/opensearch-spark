/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.opensearch.flint.spark.FlintSpark.RefreshMode.{FULL, INCREMENTAL, RefreshMode}
import org.opensearch.flint.spark.FlintSparkIndex.StreamingRefresh

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.flint.FlintDataSourceV2.FLINT_DATASOURCE
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.flint.config.FlintSparkConf.CHECKPOINT_MANDATORY
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}

/**
 * Flint spark index refresh job.
 */
class FlintSparkIndexRefresher(spark: SparkSession, flintSparkConf: FlintSparkConf)
    extends Logging {

  /**
   * Start refreshing the given index.
   *
   * @param indexName
   *   Flint index name
   * @param index
   *   Flint index
   * @param mode
   *   TODO: remove this
   * @return
   *   refresh job ID
   */
  def start(indexName: String, index: FlintSparkIndex, mode: RefreshMode): Option[String] = {
    logInfo(s"Refreshing index $indexName in $mode mode")
    val options = index.options
    val tableName = index.metadata().source

    // Batch refresh Flint index from the given source data frame
    def batchRefresh(df: Option[DataFrame] = None): Unit = {
      index
        .build(spark, df)
        .write
        .format(FLINT_DATASOURCE)
        .options(flintSparkConf.properties)
        .mode(Overwrite)
        .save(indexName)
    }

    val jobId = mode match {
      case FULL if isIncrementalRefreshing(indexName) =>
        throw new IllegalStateException(
          s"Index $indexName is incremental refreshing and cannot be manual refreshed")

      case FULL =>
        logInfo("Start refreshing index in batch style")
        batchRefresh()
        None

      // Flint index has specialized logic and capability for incremental refresh
      case INCREMENTAL if index.isInstanceOf[StreamingRefresh] =>
        logInfo("Start refreshing index in streaming style")
        val job =
          index
            .asInstanceOf[StreamingRefresh]
            .buildStream(spark)
            .writeStream
            .queryName(indexName)
            .format(FLINT_DATASOURCE)
            .options(flintSparkConf.properties)
            .addSinkOptions(options)
            .start(indexName)
        Some(job.id.toString)

      // Otherwise, fall back to foreachBatch + batch refresh
      case INCREMENTAL =>
        logInfo("Start refreshing index in foreach streaming style")
        val job = spark.readStream
          .options(options.extraSourceOptions(tableName))
          .table(tableName)
          .writeStream
          .queryName(indexName)
          .addSinkOptions(options)
          .foreachBatch { (batchDF: DataFrame, _: Long) =>
            batchRefresh(Some(batchDF))
          }
          .start()
        Some(job.id.toString)
    }

    logInfo("Refresh index complete")
    jobId
  }

  private def isIncrementalRefreshing(indexName: String): Boolean =
    spark.streams.active.exists(_.name == indexName)

  // Using Scala implicit class to avoid breaking method chaining of Spark data frame fluent API
  private implicit class FlintDataStreamWriter(val dataStream: DataStreamWriter[Row]) {

    def addSinkOptions(options: FlintSparkIndexOptions): DataStreamWriter[Row] = {
      dataStream
        .addCheckpointLocation(options.checkpointLocation())
        .addRefreshInterval(options.refreshInterval())
        .addOutputMode(options.outputMode())
        .options(options.extraSinkOptions())
    }

    def addCheckpointLocation(checkpointLocation: Option[String]): DataStreamWriter[Row] = {
      checkpointLocation match {
        case Some(location) => dataStream.option("checkpointLocation", location)
        case None if flintSparkConf.isCheckpointMandatory =>
          throw new IllegalStateException(
            s"Checkpoint location is mandatory for incremental refresh if ${CHECKPOINT_MANDATORY.key} enabled")
        case _ => dataStream
      }
    }

    def addRefreshInterval(refreshInterval: Option[String]): DataStreamWriter[Row] = {
      refreshInterval
        .map(interval => dataStream.trigger(Trigger.ProcessingTime(interval)))
        .getOrElse(dataStream)
    }

    def addOutputMode(outputMode: Option[String]): DataStreamWriter[Row] = {
      outputMode.map(dataStream.outputMode).getOrElse(dataStream)
    }
  }
}
