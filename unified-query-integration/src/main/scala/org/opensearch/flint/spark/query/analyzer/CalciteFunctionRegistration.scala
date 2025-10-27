/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query.analyzer

import org.opensearch.flint.spark.query.calcite.CalciteExecutionContext
import org.opensearch.flint.spark.query.expression.CalciteRexExpression
import org.opensearch.sql.expression.function.BuiltinFunctionName

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo

/**
 * Provides function registrations that bridge Spark function lookups to Calcite-backed
 * implementations.
 */
object CalciteFunctionRegistration {

  private val calciteBackedFunctions: Seq[BuiltinFunctionName] = Seq(
    BuiltinFunctionName.JSON_DELETE,
    BuiltinFunctionName.JSON_SET,
    BuiltinFunctionName.JSON_APPEND,
    BuiltinFunctionName.JSON_EXTEND)

  def descriptions: Seq[(FunctionIdentifier, ExpressionInfo, FunctionBuilder)] =
    calciteBackedFunctions.map { func =>
      val functionName = func.getName.getFunctionName
      val identifier = FunctionIdentifier(functionName)
      val info = new ExpressionInfo(classOf[CalciteRexExpression].getCanonicalName, functionName)
      val builder: FunctionBuilder = { children =>
        CalciteFunctionResolver
          .resolve(Seq(functionName), children, CalciteExecutionContext.getOrCreate())
          .getOrElse(throw new IllegalStateException(
            s"Unable to resolve Calcite-backed function $functionName"))
      }
      (identifier, info, builder)
    }
}
