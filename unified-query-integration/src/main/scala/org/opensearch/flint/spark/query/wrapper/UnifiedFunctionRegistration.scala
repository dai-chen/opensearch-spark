/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query.wrapper

import org.opensearch.flint.spark.query.api.{UnifiedFunctionCalciteAdapter, UnifiedFunctionRepository}
import org.opensearch.flint.spark.query.calcite.{CalciteExecutionContext, CalciteTypeConverter}
import org.opensearch.sql.expression.function.{BuiltinFunctionName, PPLFuncImpTable}

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo

/**
 * Registers Calcite-backed functions as Spark expressions implemented via
 * [[CalciteRexExpression]].
 */
object UnifiedFunctionRegistration {

  /**
   * Return Spark function descriptors backed by Calcite unified functions.
   */
  def descriptions(calciteContext: CalciteExecutionContext)
      : Seq[(FunctionIdentifier, ExpressionInfo, FunctionBuilder)] = {
    val functions =
      UnifiedFunctionRepository.loadFunctions(calciteContext).groupBy(_.functionName)

    functions.toSeq.map { case (functionName, _) =>
      val identifier = FunctionIdentifier(functionName)
      val info =
        new ExpressionInfo(classOf[UnifiedFunctionSparkWrapper].getCanonicalName, functionName)
      val builder: FunctionBuilder = { children =>
        val builtinOpt = BuiltinFunctionName.of(functionName)
        val rexBuilder = calciteContext.getRexBuilder
        val typeFactory = calciteContext.getTypeFactory
        val childRexNodes = children.zipWithIndex.map { case (child, index) =>
          val calciteType = CalciteTypeConverter.toCalciteType(child.dataType, typeFactory)
          rexBuilder.makeInputRef(calciteType, index)
        }.toArray

        val rexCall =
          PPLFuncImpTable.INSTANCE.resolve(rexBuilder, builtinOpt.get(), childRexNodes: _*)
        val unifiedFunction = UnifiedFunctionCalciteAdapter(rexCall)
        UnifiedFunctionSparkWrapper(unifiedFunction, children)
      }
      (identifier, info, builder)
    }
  }
}
