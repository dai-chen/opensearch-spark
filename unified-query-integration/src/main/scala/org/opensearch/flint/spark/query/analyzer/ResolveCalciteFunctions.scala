/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query.analyzer

import java.util.Locale

import scala.collection.JavaConverters._

import org.apache.calcite.rex.RexNode
import org.opensearch.flint.spark.query.calcite.{CalciteExecutionContext, CalciteTypeConverter}
import org.opensearch.flint.spark.query.expression.CalciteRexExpression
import org.opensearch.sql.expression.function.{BuiltinFunctionName, PPLFuncImpTable}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Custom Spark analyzer rule that intercepts unresolved function calls and resolves them to
 * CalciteRexExpression using PPLFuncImpTable.
 *
 * This rule runs BEFORE Spark's ResolveFunctions rule, allowing us to intercept PPL function
 * calls that Spark doesn't know about and delegate them to Calcite.
 *
 * The resolution strategy:
 *   1. Check if the function name exists in PPL's BuiltinFunctionName 2. Use PPLFuncImpTable to
 *      resolve the function to a RexCall 3. Wrap the RexCall in a CalciteRexExpression 4. If
 *      resolution fails, let Spark's default resolution handle it
 */
case class ResolveCalciteFunctions(spark: SparkSession) extends Rule[LogicalPlan] {

  private lazy val calciteContext = CalciteExecutionContext.getOrCreate()
  private lazy val rexBuilder = calciteContext.getRexBuilder
  private lazy val typeFactory = calciteContext.getTypeFactory
  private lazy val pplFuncImpTable = PPLFuncImpTable.INSTANCE

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan resolveOperatorsUp { case q: LogicalPlan =>
      q transformExpressionsUp {
        case u @ UnresolvedFunction(nameParts, children, _, _, _) if !u.resolved =>
          resolveToCalciteExpression(nameParts, children).getOrElse(u)
      }
    }
  }

  /**
   * Attempt to resolve a function to a CalciteRexExpression using PPLFuncImpTable. Returns None
   * if the function is not a PPL function or cannot be resolved.
   */
  private def resolveToCalciteExpression(
      nameParts: Seq[String],
      children: Seq[Expression]): Option[Expression] = {
    // Get the function name (last part if multi-part name)
    // scalastyle:off caselocale
    val funcName = nameParts.last.toLowerCase(Locale.ROOT)
    // scalastyle:on caselocale

    // Check if this is a PPL built-in function
    val pplFuncOpt = BuiltinFunctionName.of(funcName)
    if (pplFuncOpt.isEmpty) {
      return None // Not a PPL function, let Spark handle it
    }

    try {
      val pplFunc = pplFuncOpt.get()
      val normalizedChildren = CalciteFunctionResolver.normalizeArguments(pplFunc, children)

      // Convert Spark children to RexNodes
      val childRexNodes = normalizedChildren.map(childToRexNode).toArray

      // Use PPLFuncImpTable to resolve the function to a RexCall
      // This handles all the function signature matching and operator selection
      val rexCall = pplFuncImpTable.resolve(rexBuilder, pplFunc, childRexNodes: _*)

      // Wrap in CalciteRexExpression
      Some(CalciteRexExpression(rexCall, normalizedChildren, calciteContext))
    } catch {
      case e: Exception =>
        // If we can't create the RexCall, let Spark handle it
        // This might happen if the function signature doesn't match
        None
    }
  }

  /**
   * Convert a Spark Expression to a RexNode. This handles: - Literals - Attribute references -
   * Nested CalciteRexExpressions - Other resolved expressions
   */
  private def childToRexNode(expr: Expression): RexNode = {
    expr match {
      case Literal(value, dataType) =>
        // Convert Spark literal to Calcite literal
        val calciteType = CalciteTypeConverter.toCalciteType(dataType, typeFactory)
        val calciteValue = CalciteTypeConverter.sparkToCalciteValue(value, dataType)
        rexBuilder.makeLiteral(calciteValue, calciteType, true)

      case expr: CalciteRexExpression =>
        // Already a RexNode, return as-is
        expr.getRexNode

      case other if other.resolved =>
        // For resolved expressions (like AttributeReference), create an input reference
        // The index will be determined at execution time based on the expression's position
        val calciteType = CalciteTypeConverter.toCalciteType(other.dataType, typeFactory)
        // Use a placeholder index for now - this will be fixed during physical planning
        rexBuilder.makeInputRef(calciteType, 0)

      case _ =>
        // For unresolved expressions, create an ANY type placeholder
        val anyType = typeFactory.createSqlType(org.apache.calcite.sql.`type`.SqlTypeName.ANY)
        rexBuilder.makeInputRef(anyType, 0)
    }
  }
}
