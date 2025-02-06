/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.function

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}

object ApproxTopSumFunction {

  val description: (FunctionIdentifier, ExpressionInfo, FunctionBuilder) = (
    FunctionIdentifier("approx_top_sum"),
    new ExpressionInfo(
      "org.opensearch.flint.spark.function.topksketch.ApproxTopKAggSum",
      "approx_top_sum"),
    (expressions: Seq[Expression]) => {
      require(
        expressions.length == 3,
        "approx_top_sum requires three arguments: key, weight, and number")
      val keyExpr = expressions.head
      val weightExpr = expressions(1)
      val k = expressions(2).eval().asInstanceOf[Int]
      ApproxTopKAggSum(keyExpr, weightExpr, k)
    })
}
