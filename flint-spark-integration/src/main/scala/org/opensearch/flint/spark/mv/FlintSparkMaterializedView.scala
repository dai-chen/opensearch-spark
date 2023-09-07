/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.mv

import org.opensearch.flint.core.metadata.FlintMetadata
import org.opensearch.flint.spark.{FlintSpark, FlintSparkIndex, FlintSparkIndexBuilder}
import org.opensearch.flint.spark.function.TumbleFunction
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView.MV_INDEX_TYPE

import org.apache.spark.sql.{DataFrameWriter, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedFunction, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, EventTimeWatermark}
import org.apache.spark.sql.catalyst.util.IntervalUtils
import org.apache.spark.sql.flint.{dataFrameToLogicalPlan, logicalPlanToDataFrame}
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.unsafe.types.UTF8String

/**
 * Flint materialized view in Spark.
 *
 * @param mvName
 *   materialized view name
 * @param query
 *   unresolved plan
 */
class FlintSparkMaterializedView(mvName: String, query: String) extends FlintSparkIndex {

  private val tableName: String = ""

  // Hardcoding watermark delay for now
  private val watermarkDelay = UTF8String.fromString("0 Minute")

  override val kind: String = MV_INDEX_TYPE

  override def name(): String = s"flint_${tableName.replace(".", "_")}_${mvName}"

  override def metadata(): FlintMetadata =
    new FlintMetadata(s"""{
         |   "_meta": {
         |     "name": "${name()}",
         |     "kind": "$kind",
         |     "query": "$query",
         |     "source": "$tableName"
         |   },
         |   "properties": $getSchema
         | }
         |""".stripMargin)

  override def buildBatch(flint: FlintSpark): DataFrameWriter[Row] = {
    flint.spark.sql(query).write
  }

  override def buildStream(flint: FlintSpark): DataStreamWriter[Row] = {
    val batchPlan = dataFrameToLogicalPlan(flint.spark.sql(query))
    val streamingPlan = batchPlan transform {

      // Insert watermark operator between Aggregate and its child
      case Aggregate(grouping, agg, child) =>
        val timeCol = grouping.collect {
          case UnresolvedFunction(identifier, args, _, _, _)
              if identifier.mkString(".") == TumbleFunction.identifier.funcName =>
            args.head
        }

        if (timeCol.isEmpty) {
          throw new IllegalStateException(
            "Windowing function is required for streaming aggregation")
        }
        Aggregate(
          grouping,
          agg,
          EventTimeWatermark(
            timeCol.head.asInstanceOf[Attribute],
            IntervalUtils.stringToInterval(watermarkDelay),
            child))

      // Reset isStreaming flag in relation to true
      case UnresolvedRelation(multipartIdentifier, options, _) =>
        UnresolvedRelation(multipartIdentifier, options, isStreaming = true)
    }

    logicalPlanToDataFrame(flint.spark, streamingPlan).writeStream
  }

  private def getSchema: String = {
    null
  }
}

object FlintSparkMaterializedView {

  val MV_INDEX_TYPE = "mv"

  class Builder(flint: FlintSpark) extends FlintSparkIndexBuilder(flint) {
    private var mvName: String = ""
    private var query: String = ""

    def name(mvName: String): Builder = {
      this.mvName = mvName
      this
    }

    def query(query: String): Builder = {
      this.query = query
      this
    }

    override protected def buildIndex(): FlintSparkIndex = {
      new FlintSparkMaterializedView(mvName, query)
    }
  }
}
