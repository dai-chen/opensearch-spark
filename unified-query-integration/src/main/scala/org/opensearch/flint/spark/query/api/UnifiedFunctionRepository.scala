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
import org.apache.calcite.schema.impl.AggregateFunctionImpl
import org.apache.calcite.sql.SqlAggFunction
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction
import org.apache.calcite.sql.validate.SqlUserDefinedFunction
import org.opensearch.flint.spark.query.calcite.CalciteTypeConverter
import org.opensearch.flint.spark.query.wrapper.{UnifiedAggregateSparkWrapper, UnifiedFunctionSparkWrapper}
import org.opensearch.sql.calcite.udf.UserDefinedAggFunction
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory
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
    import java.util.Collections
    import org.apache.calcite.rex.RexExecutable
    import org.opensearch.sql.data.`type`.ExprType
    import org.opensearch.sql.opensearch.storage.script.CalciteScriptEngine

    val typeFactory = OpenSearchTypeFactory.TYPE_FACTORY
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
          // Spark UDF registry doesn't require specify all overloading function signatures.
          // Instead, resolve PPL function when Catalyst provides children expression during analysis in function builder.
          val rexNodes = children.map { child =>
            val calciteType = CalciteTypeConverter.toCalciteType(child.dataType, typeFactory)
            rexBuilder.makeInputRef(calciteType, children.indexOf(child))
          }
          val rexNode =
            PPLFuncImpTable.INSTANCE.resolve(rexBuilder, functionName, rexNodes.toArray: _*)

          // Pre-compile RexExecutable here to avoid Guava deserialization issues
          val rowType = {
            val rexCall = rexNode.asInstanceOf[org.apache.calcite.rex.RexCall]
            val operands = rexCall.getOperands.asScala
            if (operands.isEmpty) {
              typeFactory.createStructType(
                java.util.Collections.emptyList(),
                java.util.Collections.emptyList())
            } else {
              val inputRefs = operands.collect { case ref: org.apache.calcite.rex.RexInputRef =>
                ref
              }
              val types = inputRefs.map(_.getType).asJava
              val names = inputRefs.map(ref => s"_${ref.getIndex}").asJava
              typeFactory.createStructType(types, names)
            }
          }

          val fieldTypes = Collections.emptyMap[String, ExprType]
          val getter = new CalciteScriptEngine.ScriptInputGetter(typeFactory, rowType, fieldTypes)
          val code =
            CalciteScriptEngine.translate(rexBuilder, List(rexNode).asJava, getter, rowType)
          val rexExecutor = new RexExecutable(code, "Unified function generated code")

          val sparkDataType = CalciteTypeConverter.toSparkType(rexNode.getType)
          val isNullable = rexNode.getType.isNullable

          UnifiedFunctionSparkWrapper(rexExecutor, sparkDataType, isNullable, children)
        }
        (identifier, info, builder)
      }
  }

  /**
   * Return Spark aggregate function descriptors backed by Calcite UDAF functions. Explicitly
   * creates each UDAF instance with its merge function.
   */
  def loadAggregateFunctions(): Seq[(FunctionIdentifier, ExpressionInfo, FunctionBuilder)] = {
    import org.opensearch.sql.calcite.udf.udaf.ValuesAggFunction
    import scala.collection.JavaConverters._

    Seq(
      // VALUES aggregate function - collects distinct values in sorted order
      createAggregateFunction(
        "values",
        classOf[ValuesAggFunction],
        (
            buffer: ValuesAggFunction.ValuesAccumulator,
            input: ValuesAggFunction.ValuesAccumulator) => {
          // Get values from input and add to buffer
          val inputValues = input.value().asInstanceOf[java.util.ArrayList[String]]
          inputValues.asScala.foreach { value =>
            buffer.add(value, 0) // 0 = unlimited during merge
          }
          buffer
        })
      // Add more UDAFs here:
      // createAggregateFunction("first", new FirstAggFunction(), doMergeFirst),
    )
  }

  /**
   * Helper to create aggregate function descriptor.
   */
  private def createAggregateFunction[ACC <: UserDefinedAggFunction.Accumulator](
      functionName: String,
      udafClass: Class[_ <: UserDefinedAggFunction[ACC]],
      mergeFn: (ACC, ACC) => ACC): (FunctionIdentifier, ExpressionInfo, FunctionBuilder) = {
    val identifier = FunctionIdentifier(functionName.toLowerCase(Locale.ROOT))
    logInfo(s"Registering PPL aggregate function $identifier")

    val info = new ExpressionInfo(
      classOf[UnifiedAggregateSparkWrapper[_]].getCanonicalName,
      identifier.funcName)

    val builder: FunctionBuilder = { children =>
      UnifiedAggregateSparkWrapper(udafClass = udafClass, mergeFn = mergeFn, children = children)
    }

    (identifier, info, builder)
  }
}
