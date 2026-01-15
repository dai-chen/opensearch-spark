/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query.api

import java.util.Locale

import scala.collection.JavaConverters._

import org.opensearch.flint.spark.query.calcite.CalciteTypeConverter
import org.opensearch.flint.spark.query.wrapper.{UnifiedAggregateSparkWrapper, UnifiedFunctionSparkWrapper}
import org.opensearch.sql.api.UnifiedQueryContext
import org.opensearch.sql.api.function.{UnifiedFunction, UnifiedFunctionRepository => JavaUnifiedFunctionRepository}
import org.opensearch.sql.calcite.udf.UserDefinedAggFunction
import org.opensearch.sql.calcite.udf.udaf.ValuesAggFunction
import org.opensearch.sql.executor.QueryType

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo

/**
 * Repository that bridges the unified-query-api's UnifiedFunctionRepository with Spark's function
 * registration system.
 *
 * This utility loads PPL functions from the unified-query-api artifact and adapts them for use in
 * Spark SQL, ensuring we don't accidentally override standard Spark SQL functions.
 */
object UnifiedFunctionRepository extends Logging {

  // Lazy initialization of the Java repository with context
  @transient private lazy val javaRepository: JavaUnifiedFunctionRepository = {
    val context = UnifiedQueryContext
      .builder()
      .language(QueryType.PPL)
      .build()

    new JavaUnifiedFunctionRepository(context)
  }

  /**
   * Return Spark function descriptors backed by unified functions from the unified-query-api.
   * Filters out functions that conflict with Spark built-ins.
   */
  def loadFunctions(): Seq[(FunctionIdentifier, ExpressionInfo, FunctionBuilder)] = {
    logWarning("=== [UnifiedFunctionRepository] Loading PPL functions from unified-query-api ===")
    val descriptors = javaRepository.loadFunctions().asScala
    logWarning(
      s"=== [UnifiedFunctionRepository] Found ${descriptors.size} function descriptors ===")

    descriptors.flatMap { descriptor =>
      val functionName = descriptor.getFunctionName.toLowerCase(Locale.ROOT)
      val identifier = FunctionIdentifier(functionName)
      logWarning(s"=== [UnifiedFunctionRepository] Registering PPL function: $functionName ===")

      val info =
        new ExpressionInfo(classOf[UnifiedFunctionSparkWrapper].getCanonicalName, functionName)

      val builder: FunctionBuilder = { children =>
        // Get input types from Spark children
        val inputTypes = children.map { child =>
          CalciteTypeConverter.sparkTypeToSqlTypeName(child.dataType)
        }
        logWarning(
          s"=== [UnifiedFunctionRepository] Building function '$functionName' with input types: ${inputTypes
              .mkString(", ")} ===")

        // Build the UnifiedFunction with specific input types
        val unifiedFunction = descriptor.getBuilder.build(inputTypes.asJava)
        logWarning(
          s"=== [UnifiedFunctionRepository] Built UnifiedFunction '$functionName' -> returnType: ${unifiedFunction.getReturnType} ===")

        // Create Spark wrapper that delegates to the unified function
        UnifiedFunctionSparkWrapper(unifiedFunction, children)
      }

      Some((identifier, info, builder))
    }.toSeq
  }

  /**
   * Return Spark aggregate function descriptors backed by Calcite UDAF functions. Explicitly
   * creates each UDAF instance with its merge function.
   */
  def loadAggregateFunctions(): Seq[(FunctionIdentifier, ExpressionInfo, FunctionBuilder)] = {
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
      // Add more UDAFs here as needed
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
