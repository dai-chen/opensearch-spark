/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query.calcite

import java.util.Collections

import scala.collection.JavaConverters._

import org.apache.calcite.{DataContext, DataContexts}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.{RexBuilder, RexExecutable}
import org.opensearch.flint.spark.query.api.UnifiedFunction
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory
import org.opensearch.sql.data.`type`.ExprType
import org.opensearch.sql.expression.function.PPLFuncImpTable
import org.opensearch.sql.opensearch.storage.script.CalciteScriptEngine

import org.apache.spark.internal.Logging

/**
 * Adapter that implements UnifiedFunction interface with Calcite-specific execution logic.
 *
 * This class encapsulates all Calcite RexExecutable evaluation details, providing a clean
 * engine-agnostic interface for function execution.
 *
 * @param functionName
 *   The name of the function
 * @param rexExecutor
 *   Pre-compiled RexExecutable for function evaluation
 * @param returnTypeName
 *   SQL type name for the return value
 * @param inputTypeNames
 *   SQL type names for input parameters
 * @param isNullable
 *   Whether the return type is nullable
 */
case class UnifiedFunctionCalciteAdapter(
    override val functionName: String,
    @transient private var rexExecutor: RexExecutable,
    private val returnTypeName: String,
    private val inputTypeNames: Seq[String],
    private val isNullable: Boolean)
    extends UnifiedFunction
    with Logging {

  // Serialized form - store generated code string instead of RexExecutable
  private var serializedCode: String = _

  override def inputTypes: Seq[String] = inputTypeNames

  override def returnType: String = returnTypeName

  override def nullable: Boolean = isNullable

  /**
   * Evaluate the function with the provided inputs using Calcite execution.
   *
   * @param inputs
   *   Already evaluated argument values (in Calcite format)
   * @return
   *   The function result (in Calcite format)
   */
  override def eval(inputs: Seq[Any]): Any = {
    val dataContext = createDataContext(inputs)
    logInfo(s"Evaluating $functionName with data context: $dataContext")

    rexExecutor.setDataContext(dataContext)
    val result = rexExecutor.execute()
    if (result == null || result.isEmpty) null else result(0)
  }

  /**
   * Create a DataContext for Calcite execution from input values.
   *
   * @param inputs
   *   The input values
   * @return
   *   DataContext with field names as keys: "_0", "_1", "_2", etc.
   */
  private def createDataContext(inputs: Seq[Any]): DataContext = {
    val fieldMap = inputs.zipWithIndex.map { case (value, index) =>
      s"_$index" -> (value match {
        case null => null
        case v => v.asInstanceOf[AnyRef]
      })
    }.toMap

    DataContexts.of(fieldMap.asJava)
  }

  @throws(classOf[java.io.IOException])
  private def writeObject(out: java.io.ObjectOutputStream): Unit = {
    // Serialize the generated code from RexExecutable
    serializedCode = rexExecutor.getSource
    out.defaultWriteObject()
  }

  @throws(classOf[java.io.IOException])
  @throws(classOf[ClassNotFoundException])
  private def readObject(in: java.io.ObjectInputStream): Unit = {
    in.defaultReadObject()
    // Recreate RexExecutable from serialized code - avoids Guava cache issues
    rexExecutor = new RexExecutable(serializedCode, "Unified function generated code")
  }

  override def toString: String = s"UnifiedFunctionCalciteAdapter($functionName)"

  override def equals(obj: Any): Boolean = obj match {
    case other: UnifiedFunctionCalciteAdapter =>
      functionName == other.functionName &&
      returnTypeName == other.returnTypeName &&
      inputTypeNames == other.inputTypeNames
    case _ => false
  }

  override def hashCode(): Int = {
    var result = functionName.hashCode()
    result = 31 * result + returnTypeName.hashCode()
    result = 31 * result + inputTypeNames.hashCode()
    result
  }
}

/**
 * Factory methods for creating UnifiedFunctionCalciteAdapter instances.
 *
 * This object provides engine-agnostic factory methods that work with Calcite types only.
 */
object UnifiedFunctionCalciteAdapter extends Logging {

  /**
   * Create a UnifiedFunctionCalciteAdapter from function name and Calcite RexNode children.
   *
   * This factory method encapsulates all the complexity of:
   *   - Resolving the function from PPLFuncImpTable
   *   - Creating and compiling the RexExecutable
   *   - Extracting input and return types
   *
   * @param functionName
   *   The name of the PPL function to adapt
   * @param rexBuilder
   *   RexBuilder for creating Rex expressions
   * @param rexNodes
   *   Calcite RexNode children representing function arguments
   * @return
   *   A configured UnifiedFunctionCalciteAdapter ready for evaluation
   */
  def create(
      functionName: String,
      rexBuilder: RexBuilder,
      rexNodes: Seq[org.apache.calcite.rex.RexNode]): UnifiedFunctionCalciteAdapter = {

    val typeFactory = rexBuilder.getTypeFactory

    // Resolve the PPL function with actual argument types
    val rexNode =
      PPLFuncImpTable.INSTANCE.resolve(rexBuilder, functionName, rexNodes.toArray: _*)

    // Pre-compile RexExecutable to avoid Guava deserialization issues
    val rowType = {
      val rexCall = rexNode.asInstanceOf[org.apache.calcite.rex.RexCall]
      val operands = rexCall.getOperands.asScala
      if (operands.isEmpty) {
        typeFactory.createStructType(Collections.emptyList(), Collections.emptyList())
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

    // Extract input types from rexNodes and convert to SQL type names
    val calciteInputTypes = rexNodes.map(_.getType).toSeq
    val calciteReturnType = rexNode.getType

    // Convert RelDataType to SQL type name strings for serialization
    val inputTypeNames = calciteInputTypes.map(CalciteTypeConverter.relDataTypeToSqlTypeName)
    val returnTypeName = CalciteTypeConverter.relDataTypeToSqlTypeName(calciteReturnType)
    val isNullable = calciteReturnType.isNullable

    logInfo(s"Created UnifiedFunctionCalciteAdapter for $functionName")

    // Create the adapter with all necessary information
    UnifiedFunctionCalciteAdapter(
      functionName,
      rexExecutor,
      returnTypeName,
      inputTypeNames,
      isNullable)
  }
}
