/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query.analyzer

import java.util.Locale

import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.opensearch.flint.spark.query.calcite.{CalciteExecutionContext, CalciteTypeConverter}
import org.opensearch.flint.spark.query.expression.CalciteRexExpression
import org.opensearch.sql.expression.function.{BuiltinFunctionName, PPLFuncImpTable}

import org.apache.spark.sql.catalyst.expressions.{CreateArray, Expression, Literal}

/**
 * Helper object that resolves Spark [[UnresolvedFunction]] expressions to Calcite-backed
 * [[CalciteRexExpression]] instances using the PPL function implementation table.
 */
object CalciteFunctionResolver {

  private lazy val pplFuncImpTable = PPLFuncImpTable.INSTANCE

  private val arrayExpansionFunctions = Set(
    BuiltinFunctionName.JSON_DELETE,
    BuiltinFunctionName.JSON_SET,
    BuiltinFunctionName.JSON_APPEND,
    BuiltinFunctionName.JSON_EXTEND)

  /**
   * Attempt to resolve a function call to a Calcite expression. Returns None if the function does
   * not map to a Calcite-backed PPL function or the resolution fails.
   */
  def resolve(
      nameParts: Seq[String],
      children: Seq[Expression],
      calciteContext: CalciteExecutionContext): Option[CalciteRexExpression] = {
    val rexBuilder = calciteContext.getRexBuilder
    val typeFactory = calciteContext.getTypeFactory

    // scalastyle:off caselocale
    val funcName = nameParts.last.toLowerCase(Locale.ROOT)
    // scalastyle:on caselocale

    val pplFuncOpt = BuiltinFunctionName.of(funcName)
    if (pplFuncOpt.isEmpty) {
      return None
    }

    Try {
      val pplFunc = pplFuncOpt.get()
      val normalizedChildren = normalizeArguments(pplFunc, children)
      val childRexNodes =
        normalizedChildren.map(childToRexNode(_, rexBuilder, typeFactory, calciteContext)).toArray
      val rexCall = pplFuncImpTable.resolve(rexBuilder, pplFunc, childRexNodes: _*)
      CalciteRexExpression(rexCall, normalizedChildren, calciteContext)
    }.toOption
  }

  private[analyzer] def normalizeArguments(
      func: BuiltinFunctionName,
      args: Seq[Expression]): Seq[Expression] = {
    if (!arrayExpansionFunctions.contains(func) || args.isEmpty) {
      args
    } else {
      val head +: tail = args
      val expandedTail = tail.flatMap {
        case createArray: CreateArray => createArray.children
        case other => Seq(other)
      }
      head +: expandedTail
    }
  }

  private def childToRexNode(
      expr: Expression,
      rexBuilder: org.apache.calcite.rex.RexBuilder,
      typeFactory: org.apache.calcite.rel.`type`.RelDataTypeFactory,
      calciteContext: CalciteExecutionContext): RexNode = {
    expr match {
      case Literal(value, dataType) =>
        val calciteType = CalciteTypeConverter.toCalciteType(dataType, typeFactory)
        val calciteValue = CalciteTypeConverter.sparkToCalciteValue(value, dataType)
        rexBuilder.makeLiteral(calciteValue, calciteType, true)

      case expr: CalciteRexExpression =>
        expr.getRexNode

      case CreateArray(elements, _) =>
        val elementNodes =
          elements.map(childToRexNode(_, rexBuilder, typeFactory, calciteContext))
        rexBuilder.makeCall(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, elementNodes.asJava)

      case other if other.resolved =>
        val calciteType = CalciteTypeConverter.toCalciteType(other.dataType, typeFactory)
        // Placeholder index; actual binding happens during physical planning.
        rexBuilder.makeInputRef(calciteType, 0)

      case _ =>
        val anyType = typeFactory.createSqlType(org.apache.calcite.sql.`type`.SqlTypeName.ANY)
        rexBuilder.makeInputRef(anyType, 0)
    }
  }
}
