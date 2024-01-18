/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.opensearch.flint.spark.FlintSparkIndex.StreamingRefresh
import org.opensearch.flint.spark.FlintSparkIndexRefresher.RefreshMode.{AUTO, MANUAL}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.flint.FlintDataSourceV2.FLINT_DATASOURCE
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.flint.config.FlintSparkConf.CHECKPOINT_MANDATORY
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery, Trigger}

/**
 * Flint spark index refresher that builds and starts refreshing job.
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
   * @return
   *   refresh job ID
   */
  def start(indexName: String, index: FlintSparkIndex): Option[String] = {
    logInfo(s"Refreshing index $indexName")
    val options = index.options
    val tableName = index.metadata().source
    val mode = if (index.options.autoRefresh()) AUTO else MANUAL

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

    def streamingRefresh(): StreamingQuery = {
      index match {
        // Flint index has specialized logic and capability for incremental refresh
        case refresh: StreamingRefresh =>
          logInfo("Start refreshing index in streaming style")
          refresh
            .buildStream(spark)
            .writeStream
            .queryName(indexName)
            .format(FLINT_DATASOURCE)
            .options(flintSparkConf.properties)
            .addSinkOptions(options)
            .start(indexName)
        case _ =>
          // Otherwise, fall back to foreachBatch + batch refresh
          logInfo("Start refreshing index in foreach streaming style")
          spark.readStream
            .options(options.extraSourceOptions(tableName))
            .table(tableName)
            .writeStream
            .queryName(indexName)
            .addSinkOptions(options)
            .foreachBatch { (batchDF: DataFrame, _: Long) =>
              batchRefresh(Some(batchDF))
            }
            .start()
      }
    }

    val jobId = mode match {
      case MANUAL =>
        logInfo("Start refreshing index in batch style")
        if (options.incremental()) {
          batchRefresh()
          None
        } else {
          // Wait for Spark streaming job complete
          val job = streamingRefresh()
          job.awaitTermination()
          Some(job.id.toString)
        }
      case AUTO =>
        val job = streamingRefresh()
        Some(job.id.toString)
    }

    logInfo("Refresh index complete")
    jobId
  }

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

object FlintSparkIndexRefresher {

  /**
   * Index refresh mode: FULL: refresh on current source data in batch style at one shot
   * INCREMENTAL: auto refresh on new data in continuous streaming style
   */
  object RefreshMode extends Enumeration {
    type RefreshMode = Value
    val MANUAL, AUTO = Value
  }
}
