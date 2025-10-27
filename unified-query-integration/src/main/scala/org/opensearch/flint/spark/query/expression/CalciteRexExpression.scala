/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query.expression

import org.apache.calcite.rex.RexNode
import org.opensearch.flint.spark.query.calcite.{CalciteExecutionContext, CalciteTypeConverter}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, NonSQLExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.types.DataType
import org.apache.spark.unsafe.types.UTF8String

/**
 * A Spark expression that wraps a Calcite PPL function and executes it using Calcite's evaluation
 * infrastructure. This allows PPL functions implemented in Calcite to be executed without
 * registering individual UDFs in Spark.
 *
 * The expression is serializable by storing only the function name (String) and reconstructing
 * the RexCall at execution time, solving the problem that RexNode is not Java-serializable.
 *
 * @param pplFunctionName
 *   Name of the PPL function (e.g., "JSON_DELETE")
 * @param sparkDataType
 *   The Spark DataType of the result
 * @param isNullable
 *   Whether the result can be null
 * @param children
 *   The child Spark expressions that provide inputs to the RexNode
 */
case class CalciteRexExpression(
    pplFunctionName: String,
    sparkDataType: DataType,
    isNullable: Boolean,
    override val children: Seq[Expression])
    extends Expression
    with CodegenFallback
    with NonSQLExpression
    with Logging {

  import org.opensearch.sql.expression.function.{BuiltinFunctionName, PPLFuncImpTable}

  // Lazy execution context (transient so it won't be serialized)
  @transient private lazy val calciteContext: CalciteExecutionContext =
    CalciteExecutionContext.getOrCreate()

  // Lazily reconstruct RexNode from function name (transient so it won't be serialized)
  @transient private lazy val rexNode: RexNode = {
    val rexBuilder = calciteContext.getRexBuilder
    val typeFactory = calciteContext.getTypeFactory
    val pplFuncImpTable = PPLFuncImpTable.INSTANCE

    // Get the PPL function enum
    val pplFunc = BuiltinFunctionName.of(pplFunctionName)
      .orElseThrow(() => new IllegalStateException(s"Unknown PPL function: $pplFunctionName"))

    // Convert children to RexNodes (placeholders using proper indices)
    // Each child gets its own index (0, 1, 2, ...) to match the inputValues array at eval time
    val childRexNodes = children.zipWithIndex.map { case (child, index) =>
      val calciteType = CalciteTypeConverter.toCalciteType(child.dataType, typeFactory)
      // Use actual index - this will reference inputValues[index] in DataContext
      rexBuilder.makeInputRef(calciteType, index)
    }.toArray

    // Resolve the function to a RexCall
    pplFuncImpTable.resolve(rexBuilder, pplFunc, childRexNodes: _*)
  }

  /**
   * Get the underlying RexNode. This allows pattern matching and conversion logic to access the
   * reconstructed RexNode.
   */
  def getRexNode: RexNode = rexNode

  override def dataType: DataType = sparkDataType

  override def nullable: Boolean = isNullable

  // Mark as non-foldable to prevent Spark optimizer from trying to constant-fold
  // this expression during query optimization. Calcite expressions should only be
  // evaluated during actual execution when we have real data.
  override def foldable: Boolean = false

  /**
   * Evaluate the RexNode in interpreted mode. This is used when whole-stage codegen is not
   * available or as a fallback.
   *
   * Steps:
   * 1. Evaluate child expressions to get input values
   * 2. Convert input values to RexLiteral nodes
   * 3. Recreate the RexCall with literal values (instead of RexInputRef placeholders)
   * 4. Execute the RexCall using Calcite's RexExecutor
   * 5. Convert result back to Spark format
   */
  override def eval(input: InternalRow): Any = {
    val rexBuilder = calciteContext.getRexBuilder
    val typeFactory = calciteContext.getTypeFactory
    val pplFuncImpTable = PPLFuncImpTable.INSTANCE
    val childValues = children.map(_.eval(input))
    logInfo(s"Evaluated ${children.size} child expressions for $pplFunctionName")

    try {
      // Convert evaluated values to RexLiteral nodes (not RexInputRef!)
      val childRexLiterals = childValues.zip(children).map { case (value, expr) =>
        val calciteType = CalciteTypeConverter.toCalciteType(expr.dataType, typeFactory)
        val calciteValue = CalciteTypeConverter.sparkToCalciteValue(value, expr.dataType)
        val literal = rexBuilder.makeLiteral(calciteValue, calciteType, true)
        logInfo(s"Created literal: ${literal.toString}")
        literal
      }.toArray

      // Resolve target function
      val pplFunc = BuiltinFunctionName.of(pplFunctionName)
        .orElseThrow(() => new IllegalStateException(s"Unknown PPL function: $pplFunctionName"))

      // Resolve the function with literal values to create a RexCall
      val rexCallWithLiterals = pplFuncImpTable.resolve(rexBuilder, pplFunc, childRexLiterals: _*)
      logInfo(s"Resolved RexCall: ${rexCallWithLiterals.toString}")

      // Evaluate the RexCall (now with literals, no input references)
      val result = calciteContext.evaluate(rexCallWithLiterals, Seq.empty, dataType)
      logInfo(s"Successfully evaluated $pplFunctionName")
      result
    } catch {
      case e: Exception =>
        logError(s"Failed to evaluate CalciteRexExpression for $pplFunctionName", e)
        val fallbackResult = evaluateFallback(childValues)
        if (fallbackResult != null) {
          logInfo(s"Evaluated $pplFunctionName via fallback runtime path")
        } else {
          logDebug(s"No fallback result for $pplFunctionName")
        }
        fallbackResult
    }
  }

  private def evaluateFallback(childValues: Seq[Any]): Any = {
    val function = BuiltinFunctionName.of(pplFunctionName)
    if (!function.isPresent) {
      null
    } else {
      function.get() match {
        case BuiltinFunctionName.JSON_DELETE =>
          evaluateJsonDeleteFallback(childValues)
        case other =>
          logDebug(s"No fallback runtime available for Calcite function $other")
          null
      }
    }
  }

  private def evaluateJsonDeleteFallback(childValues: Seq[Any]): Any = {
    if (childValues.isEmpty) {
      null
    } else {
      toNonNullString(childValues.head) match {
        case None => null
        case Some(json) =>
          val keys = childValues.tail.flatMap(toNonNullString)
          CalciteJsonFunctionFallback
            .jsonDelete(json, keys)
            .map(UTF8String.fromString)
            .orNull
      }
    }
  }

  private def toNonNullString(value: Any): Option[String] = {
    value match {
      case null => None
      case utf8: UTF8String =>
        Option(utf8.toString)
      case other =>
        Option(other.toString)
    }
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

  import org.apache.calcite.rex.RexCall

  /**
   * Create a CalciteRexExpression from a RexNode, extracting the function name for serialization.
   *
   * @param rexNode
   *   The Calcite RexNode to wrap (must be a RexCall)
   * @param children
   *   The child Spark expressions
   * @param calciteContext
   *   The Calcite execution context for type conversion
   * @return
   *   A serializable CalciteRexExpression
   */
  def apply(
      rexNode: RexNode,
      children: Seq[Expression],
      calciteContext: CalciteExecutionContext): CalciteRexExpression = {

    // Extract function name from RexCall
    val functionName = rexNode match {
      case rexCall: RexCall =>
        rexCall.getOperator.getName
      case _ =>
        throw new IllegalArgumentException(
          s"CalciteRexExpression only supports RexCall nodes, got: ${rexNode.getClass}")
    }

    // Extract type information
    val sparkDataType = CalciteTypeConverter.toSparkType(rexNode.getType)
    val isNullable = rexNode.getType.isNullable

    // Create the expression with serializable data (just the function name string)
    new CalciteRexExpression(functionName, sparkDataType, isNullable, children)
  }

  /**
   * Create a CalciteRexExpression with a shared execution context.
   */
  def apply(rexNode: RexNode, children: Seq[Expression]): CalciteRexExpression = {
    apply(rexNode, children, CalciteExecutionContext.getOrCreate())
  }
}
