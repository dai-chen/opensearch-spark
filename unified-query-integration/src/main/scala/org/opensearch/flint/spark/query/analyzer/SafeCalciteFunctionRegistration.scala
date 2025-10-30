/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query.analyzer

import java.util.Locale

import scala.util.Try

import org.opensearch.flint.spark.query.calcite.CalciteExecutionContext
import org.opensearch.flint.spark.query.wrapper.UnifiedFunctionRegistration

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo

/**
 * Provides safe registration of Calcite functions that avoids overriding Spark SQL built-ins.
 *
 * This utility checks if a function already exists in Spark's built-in function registry before
 * registering the Calcite version, ensuring we don't accidentally override standard Spark SQL
 * functions.
 */
object SafeCalciteFunctionRegistration extends Logging {

  /**
   * Check if a function exists in Spark's built-in function registry.
   *
   * Note: This only checks built-in functions, not session-level temporary functions or catalog
   * functions, as those don't exist at extension registration time.
   */
  def isSparkBuiltinFunction(functionName: String): Boolean = {
    Try {
      FunctionRegistry.expressions.contains(functionName.toLowerCase(Locale.ROOT))
    }.getOrElse(false)
  }

  /**
   * Get safe function descriptions that won't conflict with Spark built-ins.
   *
   * This method filters UnifiedFunctionRegistration.descriptions based on the provided
   * configuration, ensuring we only register functions that are safe.
   */
  def getSafeDescriptions(
      calciteContext: CalciteExecutionContext = CalciteExecutionContext.getOrCreate())
      : Seq[(FunctionIdentifier, ExpressionInfo, FunctionRegistry.FunctionBuilder)] = {

    UnifiedFunctionRegistration.descriptions(calciteContext).flatMap {
      case (identifier, info, builder) =>
        val functionName = identifier.funcName
        val isBuiltin = isSparkBuiltinFunction(functionName)

        if (!isBuiltin) {
          logInfo(s"Registering PPL function $identifier")
          Some((identifier, info, builder))
        } else {
          None
        }
    }
  }
}
