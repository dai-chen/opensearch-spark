/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query.calcite

import scala.collection.JavaConverters._

import org.apache.calcite.rex.{RexBuilder, RexCall, RexLiteral, RexNode}
import org.opensearch.sql.expression.function.{BuiltinFunctionName, PPLFuncImpTable}

import org.apache.spark.internal.Logging

/**
 * Serializes and deserializes Calcite RexNode expressions to/from a simple string format.
 *
 * This enables RexNode to be serialized for Spark's distributed execution, solving the problem
 * that RexNode is not directly Java-serializable.
 *
 * For PPL functions (RexCall), we store the function name and can recreate it via PPLFuncImpTable.
 * This avoids complex Calcite JSON serialization and leverages our existing resolution mechanism.
 */
class RexJsonSerializer(calciteContext: CalciteExecutionContext) extends Logging with Serializable {

  private val pplFuncImpTable = PPLFuncImpTable.INSTANCE

  /**
   * Serialize a RexNode to a simple string representation.
   *
   * For RexCall nodes representing PPL functions, we serialize as: "PPL_FUNC:function_name"
   *
   * @param rexNode
   *   The RexNode to serialize
   * @return
   *   String representation
   */
  def serialize(rexNode: RexNode): String = {
    rexNode match {
      case rexCall: RexCall =>
        // Get the operator name (function name)
        val operatorName = rexCall.getOperator.getName
        s"PPL_FUNC:$operatorName"

      case _ =>
        // For other types, use toString (suboptimal but rare case)
        s"REX_STRING:${rexNode.toString}"
    }
  }

  /**
   * Deserialize a RexNode from string representation.
   *
   * Since RexNode needs to be rebuilt with proper context, this is a placeholder. In practice, we
   * avoid deserialization by reconstructing from the function name at execution time.
   *
   * @param serialized
   *   Serialized string representation
   * @return
   *   Deserialized RexNode (throws exception - reconstruction happens at execution time)
   */
  def deserialize(serialized: String): RexNode = {
    throw new UnsupportedOperationException(
      "RexNode deserialization should not be called directly. " +
        "CalciteRexExpression reconstructs the function at execution time using PPLFuncImpTable.")
  }

  /**
   * Extract the function name from serialized representation.
   */
  def extractFunctionName(serialized: String): Option[String] = {
    if (serialized.startsWith("PPL_FUNC:")) {
      Some(serialized.stripPrefix("PPL_FUNC:"))
    } else {
      None
    }
  }
}

/**
 * Companion object for creating RexJsonSerializer instances.
 */
object RexJsonSerializer {

  /**
   * Create a RexJsonSerializer with a shared execution context.
   */
  def apply(calciteContext: CalciteExecutionContext): RexJsonSerializer = {
    new RexJsonSerializer(calciteContext)
  }

  /**
   * Create a RexJsonSerializer with default context.
   */
  def apply(): RexJsonSerializer = {
    new RexJsonSerializer(CalciteExecutionContext.getOrCreate())
  }
}
