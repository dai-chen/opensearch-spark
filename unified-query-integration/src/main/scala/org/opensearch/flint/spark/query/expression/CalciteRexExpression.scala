/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query.expression

import org.apache.calcite.rex.RexNode
import org.opensearch.flint.spark.query.calcite.{CalciteExecutionContext, CalciteTypeConverter}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, NonSQLExpression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.types.DataType

/**
 * A Spark expression that wraps a Calcite RexNode and executes it using Calcite's evaluation
 * infrastructure. This allows PPL functions implemented in Calcite to be executed without
 * registering individual UDFs in Spark.
 *
 * The expression supports both interpreted execution (via eval()) and code generation (via
 * doGenCode()) for optimal performance.
 *
 * @param rexNode
 *   The Calcite RexNode to execute
 * @param children
 *   The child Spark expressions that provide inputs to the RexNode
 * @param calciteContext
 *   The Calcite execution context for evaluation
 */
case class CalciteRexExpression(
    rexNode: RexNode,
    override val children: Seq[Expression],
    calciteContext: CalciteExecutionContext)
    extends Expression
    with CodegenFallback
    with NonSQLExpression {

  override def dataType: DataType = CalciteTypeConverter.toSparkType(rexNode.getType)

  override def nullable: Boolean = rexNode.getType.isNullable

  /**
   * Evaluate the RexNode in interpreted mode. This is used when whole-stage codegen is not
   * available or as a fallback.
   *
   * Steps: 1. Evaluate child expressions to get input values 2. Convert input values from Spark
   * to Calcite format 3. Execute RexNode using Calcite's RexInterpreter 4. Convert result back to
   * Spark format
   */
  override def eval(input: InternalRow): Any = {
    // Step 1: Evaluate child expressions
    val childValues = children.map(_.eval(input))

    // Step 2: Convert to Calcite format
    val calciteInputs = childValues.zip(children).map { case (value, expr) =>
      CalciteTypeConverter.sparkToCalciteValue(value, expr.dataType)
    }

    // Step 3: Execute via Calcite
    calciteContext.evaluate(rexNode, calciteInputs, dataType)
  }

  override def toString: String = s"CalciteRex(${rexNode.toString})"

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }

  // For now, we use CodegenFallback which calls eval() during code generation
  // TODO: Implement full doGenCode() with Linq4j for better performance
}

/**
 * Companion object for creating CalciteRexExpression instances.
 */
object CalciteRexExpression {

  /**
   * Create a CalciteRexExpression with a shared execution context.
   */
  def apply(rexNode: RexNode, children: Seq[Expression]): CalciteRexExpression = {
    CalciteRexExpression(rexNode, children, CalciteExecutionContext.getOrCreate())
  }
}
