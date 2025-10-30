/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query.wrapper

import org.opensearch.flint.spark.query.api.UnifiedFunction
import org.opensearch.flint.spark.query.calcite.CalciteTypeConverter

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, NonSQLExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.DataType

/**
 * Thin Spark expression wrapper that delegates evaluation to a [[UnifiedFunction]] instance.
 */
case class UnifiedFunctionSparkWrapper(
    unifiedFunction: UnifiedFunction,
    override val children: Seq[Expression])
    extends Expression
    with CodegenFallback
    with NonSQLExpression {

  require(
    unifiedFunction.inputTypes.size == children.size,
    s"UnifiedFunction expected ${unifiedFunction.inputTypes.size} arguments, " +
      s"but Spark wrapper received ${children.size}")

  override def dataType: DataType =
    CalciteTypeConverter.toSparkType(unifiedFunction.returnType)

  override def nullable: Boolean = unifiedFunction.nullable

  override def foldable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val sparkValues = children.map(_.eval(input))
    val calciteInputs = sparkValues.zip(children).map { case (value, expr) =>
      CalciteTypeConverter.sparkToCalciteValue(value, expr.dataType)
    }

    val calciteResult = unifiedFunction.eval(calciteInputs)
    CalciteTypeConverter.calciteToSparkValue(calciteResult, dataType)
  }

  override def toString: String = s"UnifiedFunction(${unifiedFunction.getClass.getSimpleName})"

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }
}
