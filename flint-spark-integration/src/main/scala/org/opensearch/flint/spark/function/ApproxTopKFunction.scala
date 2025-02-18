/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.function

import org.opensearch.flint.spark.function.topksketch.TopKSketch

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}
import org.apache.spark.sql.types.IntegerType

/**
 * Registers a Top-K aggregate function with a custom sketch implementation.
 */
object ApproxTopKFunction {

  /**
   * Create a function description for Spark.
   *
   * @param functionName
   *   The name of the function to register (e.g., "approx_top_count_accurate")
   * @param createSketch
   *   A factory method to create the desired Top-K sketch
   * @return
   *   A function description tuple (identifier, info, builder)
   */
  def apply(functionName: String, createSketch: (Int, Int) => TopKSketch[String])
      : (FunctionIdentifier, ExpressionInfo, FunctionBuilder) = {
    val identifier = FunctionIdentifier(functionName)

    val exprInfo = new ExpressionInfo(
      classOf[ApproxTopKAgg].getCanonicalName,
      functionName,
      s"Approximates the Top-K values using the $functionName algorithm.")

    val functionBuilder: Seq[Expression] => Expression = (children: Seq[Expression]) => {
      require(children.size == 3, s"$functionName requires exactly 3 arguments: (expr, k)")
      val expr = children.head
      val kExpr = children(1)
      val trackedExpr = children(2)

      if (kExpr.dataType != IntegerType || trackedExpr.dataType != IntegerType) {
        throw new IllegalArgumentException(
          s"The second and third argument to $functionName must be an integer.")
      }

      val k = kExpr.eval().asInstanceOf[Int]
      val tracked = trackedExpr.eval().asInstanceOf[Int]
      ApproxTopKAgg(expr, k, tracked, (k, tracked) => createSketch(k, tracked))
    }

    (identifier, exprInfo, functionBuilder)
  }
}
