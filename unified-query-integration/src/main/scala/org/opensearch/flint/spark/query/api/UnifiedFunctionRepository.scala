/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query.api

import java.util.Locale

import scala.util.Try

import org.apache.calcite.sql.`type`.SqlTypeName
import org.opensearch.flint.spark.query.calcite.CalciteExecutionContext
import org.opensearch.sql.expression.function.{BuiltinFunctionName, PPLFuncImpTable}
import org.slf4j.LoggerFactory

/**
 * Repository that inspects the Calcite PPL function implementation table and exposes unified
 * functions that can be adapted for Spark or other engines.
 */
object UnifiedFunctionRepository {

  private val logger = LoggerFactory.getLogger(UnifiedFunctionRepository.getClass)

  case class Entry(functionName: String, function: UnifiedFunction)

  private val pplFuncImpTable = PPLFuncImpTable.INSTANCE

  /**
   * Enumerate Calcite functions available from [[PPLFuncImpTable]] and convert them into
   * [[UnifiedFunction]] instances when possible.
   */
  def loadFunctions(calciteContext: CalciteExecutionContext): Seq[Entry] = {
    val rexBuilder = calciteContext.getRexBuilder
    val typeFactory = calciteContext.getTypeFactory
    val anyType = typeFactory.createSqlType(SqlTypeName.ANY)

    BuiltinFunctionName.values().toSeq.flatMap { builtinName =>
      val functionKey = builtinName.getName.getFunctionName.toLowerCase(Locale.ROOT)
      val adapters = (0 to MaxArity).flatMap { arity =>
        val args = (0 until arity).map(index => rexBuilder.makeInputRef(anyType, index)).toArray
        Try {
          val rexCall = pplFuncImpTable.resolve(rexBuilder, builtinName, args: _*)
          UnifiedFunctionCalciteAdapter(rexCall)
        }.toOption
      }

      adapters.headOption.map(adapter => Entry(functionKey, adapter))
    }
  }

  private val MaxArity = 6
}
