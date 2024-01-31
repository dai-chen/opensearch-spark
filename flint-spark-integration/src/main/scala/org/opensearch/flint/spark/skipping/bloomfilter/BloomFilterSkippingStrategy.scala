/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.bloomfilter

import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.SkippingKind.{BLOOM_FILTER, SkippingKind}

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, Literal}
import org.apache.spark.sql.functions._

case class BloomFilterSkippingStrategy(
    override val kind: SkippingKind = BLOOM_FILTER,
    override val columnName: String,
    override val columnType: String)
    extends FlintSparkSkippingStrategy {

  override def outputSchema(): Map[String, String] = Map(columnName -> "binary")

  override def getAggregators: Seq[Expression] = {
    /*
    val K = 1024L
    val ranges = Array(1, 2, 4, 8, 16, 32, 64, 128, 256, 512)
    val bfAggs = ranges.map(_ * K).map(bloomFilterAgg)
    Seq(array_min(array(bfAggs: _*)).expr)
    */
    Seq(bloomFilterAgg(100).expr)
  }

  private def bloomFilterAgg(expectNDV: Long): Column = {
    new Column(
      new BloomFilterAggregate(xxhash64(col(columnName)).expr, lit(expectNDV).expr)
        .toAggregateExpression())
  }

  override def rewritePredicate(predicate: Expression): Option[Expression] = {
    predicate match {
      case EqualTo(AttributeReference(`columnName`, _, _, _), value: Literal) =>
        Some(BloomFilterMightContain(col(columnName).expr, xxhash64(new Column(value)).expr))
      case _ => None
    }
  }
}
