/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query.wrapper

import org.opensearch.flint.spark.query.api.UnifiedFunction
import org.opensearch.flint.spark.query.calcite.CalciteTypeConverter

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, NonSQLExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.DataType

/**
 * Spark expression wrapper that delegates to UnifiedFunction for evaluation.
 *
 * This wrapper handles Spark-specific concerns (expression tree, type conversion) while
 * delegating the actual function evaluation to the engine-agnostic UnifiedFunction.
 */
case class UnifiedFunctionSparkWrapper(
    unifiedFunction: UnifiedFunction,
    override val children: Seq[Expression])
    extends Expression
    with CodegenFallback
    with NonSQLExpression
    with Logging {

  override def dataType: DataType =
    CalciteTypeConverter.sqlTypeNameToSparkType(unifiedFunction.returnType)

  override def nullable: Boolean = unifiedFunction.nullable

  override def foldable: Boolean = false

  override def eval(input: InternalRow): Any = {
    // Evaluate child expressions to get Spark values
    val sparkValues = children.map(_.eval(input))

    // Convert Spark values to Calcite format
    val calciteInputs = sparkValues.zip(children).map { case (value, expr) =>
      CalciteTypeConverter.sparkToCalciteValue(value, expr.dataType)
    }

    // Delegate to UnifiedFunction for evaluation
    val calciteResult = unifiedFunction.eval(calciteInputs)

    // Convert Calcite result back to Spark format
    CalciteTypeConverter.calciteToSparkValue(calciteResult, dataType)
  }

  override def toString: String = s"${unifiedFunction.functionName}(${children.mkString(",")})"

  override lazy val canonicalized: Expression = {
    val canonicalizedChildren = children.map(_.canonicalized)
    copy(children = canonicalizedChildren)
  }

  override def equals(obj: Any): Boolean = obj match {
    case other: UnifiedFunctionSparkWrapper =>
      unifiedFunction == other.unifiedFunction &&
      children == other.children
    case _ => false
  }

  override def hashCode(): Int = {
    var result = unifiedFunction.hashCode()
    result = 31 * result + children.hashCode()
    result
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }
}
