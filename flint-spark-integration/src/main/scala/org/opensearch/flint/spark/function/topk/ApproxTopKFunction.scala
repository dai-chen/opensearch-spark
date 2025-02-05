/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.function.topk

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}
import org.apache.spark.sql.types.IntegerType

/**
 * Approximate Top-K function that finds the most common values for a given expression.
 */
object ApproxTopKFunction {

  /**
   * Function name.
   */
  val identifier: FunctionIdentifier = FunctionIdentifier("approx_top_count")

  /**
   * Function signature: returns an array of structs containing the top K values and their counts.
   */
  val exprInfo: ExpressionInfo = new ExpressionInfo(
    classOf[Column].getCanonicalName,
    identifier.funcName,
    "Finds the approximate Top-K values using a Count-Min Sketch.")

  /**
   * Function implementation builder.
   */
  val functionBuilder: Seq[Expression] => Expression = (children: Seq[Expression]) => {
    // Validate argument count and types
    require(
      children.size >= 2 && children.size <= 3,
      "approx_top_count requires 2 or 3 arguments: approx_top_count(<expr>, <k>, [counters])")

    val expr = children.head
    val kExpr = children(1)
    val countersExpr = if (children.size == 3) Some(children(2)) else None

    if (kExpr.dataType != IntegerType) {
      throw new IllegalArgumentException("The second argument <k> must be an integer.")
    }

    // Extract K and optional counters
    val k = kExpr.eval().asInstanceOf[Int]
    val counters = countersExpr.map(_.eval().asInstanceOf[Int]).getOrElse(100000)

    // Create and return the TopKCMSAgg expression
    ApproxTopKAgg(expr, k)
  }

  /**
   * Function description for registering in a Spark extension.
   */
  val description: (FunctionIdentifier, ExpressionInfo, FunctionBuilder) =
    (identifier, exprInfo, functionBuilder)
}
