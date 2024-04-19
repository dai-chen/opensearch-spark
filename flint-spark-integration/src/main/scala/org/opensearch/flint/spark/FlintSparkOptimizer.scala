/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.opensearch.flint.spark.covering.ApplyFlintSparkCoveringIndex
import org.opensearch.flint.spark.skipping.ApplyFlintSparkSkippingIndex

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.flint.config.FlintSparkConf

/**
 * Flint Spark optimizer that manages all Flint related optimizer rule.
 * @param spark
 *   Spark session
 */
class FlintSparkOptimizer(spark: SparkSession) extends Rule[LogicalPlan] {

  /** Flint Spark API */
  private val flint: FlintSpark = new FlintSpark(spark)

  /** Only one Flint optimizer rule for now. Need to estimate cost if more than one in future. */
  private val rules =
    Seq(new ApplyFlintSparkCoveringIndex(flint), new ApplyFlintSparkSkippingIndex(flint))

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (FlintSparkConf().isCoveringIndexRewriteEnabled) {
      rules.head.apply(plan) // TODO: apply one by one
    } else {
      plan
    }
  }

  private def isOptimizerEnabled: Boolean = {
    FlintSparkConf().isOptimizerEnabled
  }
}
