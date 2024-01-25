/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.bloomfilter

import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.SkippingKind.{BLOOM_FILTER, SkippingKind}

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, Literal}
import org.apache.spark.sql.functions.{col, lit, xxhash64}

case class BloomFilterSkippingStrategy(
    override val kind: SkippingKind = BLOOM_FILTER,
    override val columnName: String,
    override val columnType: String)
    extends FlintSparkSkippingStrategy {

  override def outputSchema(): Map[String, String] = Map(columnName -> "binary")

  override def getAggregators: Seq[Expression] = {
    Seq(
      new BloomFilterAggregate(xxhash64(col(columnName)).expr, lit(1000L).expr)
        .toAggregateExpression())
  }

  override def rewritePredicate(predicate: Expression): Option[Expression] = {
    predicate match {
      case EqualTo(AttributeReference(`columnName`, _, _, _), value: Literal) =>
        Some(BloomFilterMightContain(col(columnName).expr, value))
      case _ => None
    }
  }
}
