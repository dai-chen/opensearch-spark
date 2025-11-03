/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query.api

import java.util.Locale

import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.calcite.jdbc.JavaTypeFactoryImpl
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.sql.validate.SqlUserDefinedFunction
import org.opensearch.flint.spark.query.calcite.CalciteTypeConverter
import org.opensearch.flint.spark.query.wrapper.UnifiedFunctionSparkWrapper
import org.opensearch.sql.expression.function.{PPLBuiltinOperators, PPLFuncImpTable}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo

/**
 * Repository that inspects the Calcite PPL function implementation table and exposes unified
 * functions that can be adapted for Spark or other engines.
 *
 * This utility checks if a function already exists in Spark's built-in function registry before
 * registering the Calcite version, ensuring we don't accidentally override standard Spark SQL
 * functions.
 */
object UnifiedFunctionRepository extends Logging {

  /**
   * Return Spark function descriptors backed by Calcite unified functions. Filters out functions
   * that conflict with Spark built-ins. Function resolution via PPLFuncImplTable.resolve()
   * happens at runtime when actual argument types are available, avoiding NPE from paramTypes().
   */
  def loadFunctions(): Seq[(FunctionIdentifier, ExpressionInfo, FunctionBuilder)] = {
    val typeFactory = new JavaTypeFactoryImpl()
    val rexBuilder = new RexBuilder(typeFactory)

    val operatorTable = PPLBuiltinOperators.instance()
    val operators = operatorTable.getOperatorList.asScala.collect {
      case udf: SqlUserDefinedFunction => udf
    }

    operators
      .map { function =>
        val functionName = function.getName.toLowerCase(Locale.ROOT)
        val identifier = FunctionIdentifier(functionName)
        logInfo(s"Registering PPL function $identifier")
        val info =
          new ExpressionInfo(classOf[UnifiedFunctionSparkWrapper].getCanonicalName, functionName)
        val builder: FunctionBuilder = { children =>
          // Convert Spark children expressions to Calcite RexNodes with proper types
          val rexNodes = children.map { child =>
            val calciteType = CalciteTypeConverter.toCalciteType(child.dataType, typeFactory)
            rexBuilder.makeInputRef(calciteType, children.indexOf(child))
          }

          // Use PPLFuncImplTable.resolve() with properly-typed arguments
          val rexNode =
            PPLFuncImpTable.INSTANCE.resolve(rexBuilder, functionName, rexNodes.toArray: _*)

          // Directly wrap RexNode in UnifiedFunctionSparkWrapper
          UnifiedFunctionSparkWrapper(rexNode, children)
        }
        (identifier, info, builder)
      }
  }
}
