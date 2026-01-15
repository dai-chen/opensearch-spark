/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query.wrapper

import scala.collection.JavaConverters._

import org.opensearch.flint.spark.query.calcite.CalciteTypeConverter
import org.opensearch.sql.api.function.UnifiedFunction

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, NonSQLExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.types.DataType

/**
 * Spark expression wrapper that delegates to UnifiedFunction for evaluation.
 *
 * This wrapper handles Spark-specific concerns (expression tree, type conversion) while
 * delegating the actual function evaluation to the engine-agnostic UnifiedFunction from the
 * unified-query-api artifact.
 *
 * Supports both interpreted evaluation (eval) and whole-stage codegen (doGenCode) by integrating
 * Calcite's pre-compiled Linq4j code with Spark's codegen framework.
 */
case class UnifiedFunctionSparkWrapper(
    unifiedFunction: UnifiedFunction,
    override val children: Seq[Expression])
    extends Expression
    with CodegenFallback
    with NonSQLExpression
    with Logging {

  override def dataType: DataType =
    CalciteTypeConverter.sqlTypeNameToSparkType(unifiedFunction.getReturnType)

  // UnifiedFunction doesn't expose nullable, default to true for safety
  override def nullable: Boolean = true

  override def foldable: Boolean = false

  override def eval(input: InternalRow): Any = {
    logWarning(
      s"=== [UnifiedFunctionSparkWrapper.eval] Evaluating '${unifiedFunction.getFunctionName}' ===")

    // Evaluate child expressions to get Spark values
    val sparkValues = children.map(_.eval(input))

    // Convert Spark values to Calcite format
    val calciteInputs = sparkValues.zip(children).map { case (value, expr) =>
      CalciteTypeConverter.sparkToCalciteValue(value, expr.dataType).asInstanceOf[Object]
    }

    // Delegate to UnifiedFunction for evaluation (Java List[Object] expected)
    val calciteResult = unifiedFunction.eval(calciteInputs.asJava)

    // Convert Calcite result back to Spark format
    CalciteTypeConverter.calciteToSparkValue(calciteResult, dataType)
  }

  /*
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    logWarning(
      s"=== [UnifiedFunctionSparkWrapper.doGenCode] Generating code for '${unifiedFunction.getFunctionName}' ===")

    // Add UnifiedFunction as a reference object accessible in generated code
    val funcRef =
      ctx.addReferenceObj("unifiedFunc", unifiedFunction, classOf[UnifiedFunction].getName)

    // Add CalciteTypeConverter as a reference for type conversions
    val converterClass = CalciteTypeConverter.getClass.getName.stripSuffix("$")

    // Generate code for all children
    val childrenGen = children.map(_.genCode(ctx))

    // Create variable names
    val inputArray = ctx.freshName("inputArray")
    val calciteResult = ctx.freshName("calciteResult")
    val inputList = ctx.freshName("inputList")
    val convertedResult = ctx.freshName("convertedResult")

    // Build code to populate input array with converted values
    val buildInputsCode = childrenGen.zipWithIndex
      .map { case (childGen, i) =>
        val childType = children(i).dataType
        val converterMethod = s"$converterClass.sparkToCalciteValue"
        val sparkType = ctx.addReferenceObj("sparkType" + i, childType, classOf[DataType].getName)
        s"""
         |${childGen.code}
         |if (${childGen.isNull}) {
         |  $inputArray[$i] = null;
         |} else {
         |  $inputArray[$i] = $converterMethod(${childGen.value}, $sparkType);
         |}
       """.stripMargin
      }
      .mkString("\n")

    // Generate code to convert result back to Spark format
    val resultType = ctx.addReferenceObj("resultType", dataType, classOf[DataType].getName)
    val resultConverterMethod = s"$converterClass.calciteToSparkValue"

    // Get the proper Java type for the result
    val javaType = ctx.javaType(dataType)
    val defaultValue = ctx.defaultValue(dataType)

    val code =
      s"""
         |// Build input array for UnifiedFunction
         |Object[] $inputArray = new Object[${children.size}];
         |$buildInputsCode
         |
         |// Convert to Java List and call UnifiedFunction.eval()
         |java.util.List $inputList = java.util.Arrays.asList($inputArray);
         |Object $calciteResult = $funcRef.eval($inputList);
         |
         |// Convert Calcite result back to Spark format with proper type cast
         |boolean ${ev.isNull} = ($calciteResult == null);
         |$javaType ${ev.value} = $defaultValue;
         |if (!${ev.isNull}) {
         |  Object $convertedResult = $resultConverterMethod($calciteResult, $resultType);
         |  ${ev.value} = ($javaType) $convertedResult;
         |}
       """.stripMargin

    ev.copy(code = code"$code")
  }
   */

  override def toString: String = s"${unifiedFunction.getFunctionName}(${children.mkString(",")})"

  override lazy val canonicalized: Expression = {
    val canonicalizedChildren = children.map(_.canonicalized)
    copy(children = canonicalizedChildren)
  }

  override def equals(obj: Any): Boolean = obj match {
    case other: UnifiedFunctionSparkWrapper =>
      unifiedFunction == other.unifiedFunction &&
      children == other.children
    case _ => false
  }

  override def hashCode(): Int = {
    var result = unifiedFunction.hashCode()
    result = 31 * result + children.hashCode()
    result
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }
}
