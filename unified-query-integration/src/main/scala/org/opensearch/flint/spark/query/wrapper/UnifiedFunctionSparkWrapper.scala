/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query.wrapper

import scala.collection.JavaConverters._

import org.apache.calcite.{DataContext, DataContexts}
import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.jdbc.JavaTypeFactoryImpl
import org.apache.calcite.plan.{RelOptCluster, RelOptPlanner}
import org.apache.calcite.plan.volcano.VolcanoPlanner
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.{RexBuilder, RexCall, RexExecutorImpl, RexInputRef, RexNode}
import org.opensearch.flint.spark.query.calcite.CalciteTypeConverter
import org.opensearch.sql.opensearch.storage.serde.RelJsonSerializer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, NonSQLExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.DataType

/**
 * Spark expression wrapper that directly evaluates Calcite RexNode expressions. Handles
 * serialization/deserialization using OpenSearch's RelJsonSerializer for safe distribution across
 * executors.
 */
case class UnifiedFunctionSparkWrapper(
    @transient private var _rexNode: RexNode,
    override val children: Seq[Expression])
    extends Expression
    with CodegenFallback
    with NonSQLExpression
    with Logging {

  // Serialized form
  private var serializedData: String = _

  @transient private lazy val typeFactory: JavaTypeFactory = new JavaTypeFactoryImpl()
  @transient private lazy val rexBuilder: RexBuilder = new RexBuilder(typeFactory)
  @transient private lazy val planner: RelOptPlanner = new VolcanoPlanner()
  @transient private lazy val cluster: RelOptCluster = {
    planner.setExecutor(null)
    RelOptCluster.create(planner, rexBuilder)
  }
  @transient private lazy val relJsonSerializer: RelJsonSerializer = new RelJsonSerializer(
    cluster)
  @transient private lazy val rowType: RelDataType = extractInputSchema(_rexNode)

  // Cache the compiled executor - expensive to create as it generates and compiles Java code
  @transient private lazy val rexExecutor = {
    val executor = RexExecutorImpl.getExecutable(
      rexBuilder,
      java.util.Collections.singletonList(_rexNode),
      rowType)
    logInfo(s"Generate code: ${executor.getSource}")
    executor
  }

  override def dataType: DataType = CalciteTypeConverter.toSparkType(_rexNode.getType)

  override def nullable: Boolean = _rexNode.getType.isNullable

  override def foldable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val sparkValues = children.map(_.eval(input))
    val calciteInputs = sparkValues.zip(children).map { case (value, expr) =>
      CalciteTypeConverter.sparkToCalciteValue(value, expr.dataType)
    }

    val calciteResult = evaluateRexNode(calciteInputs)
    CalciteTypeConverter.calciteToSparkValue(calciteResult, dataType)
  }

  private def evaluateRexNode(inputs: Seq[Any]): Any = {
    val dataContext = createDataContext(inputs)
    logInfo(s"Data context: $dataContext")

    rexExecutor.setDataContext(dataContext)
    val result = rexExecutor.execute()
    if (result == null || result.isEmpty) null else result(0)
  }

  private def createDataContext(inputs: Seq[Any]): DataContext = {
    val valuesArray: Array[AnyRef] = inputs
      .map {
        case null => null
        case v => v.asInstanceOf[AnyRef]
      }
      .toArray[AnyRef]
    DataContexts.of(Map("inputRecord" -> valuesArray).asJava)
  }

  /**
   * Extract input schema (RelDataType) from RexCall operands. Since all inputs in
   * UnifiedFunctionRepository.loadFunctions() are RexInputRef created with makeInputRef(), we can
   * directly extract types from the call's operands.
   */
  private def extractInputSchema(rexNode: RexNode): RelDataType = {
    val rexCall = rexNode.asInstanceOf[RexCall]
    val operands = rexCall.getOperands.asScala

    if (operands.isEmpty) {
      typeFactory.createStructType(
        java.util.Collections.emptyList(),
        java.util.Collections.emptyList())
    } else {
      // All operands are RexInputRef from loadFunctions()
      val inputRefs = operands.collect { case ref: RexInputRef => ref }
      val types = inputRefs.map(_.getType).asJava
      val names = inputRefs.map(ref => s"_${ref.getIndex}").asJava
      typeFactory.createStructType(types, names)
    }
  }

  /**
   * Serialize RexNode using RelJsonSerializer. fieldTypes parameter is empty since it's
   * OpenSearch-specific (ExprType).
   */
  private def serialize(rexNode: RexNode): String = {
    try {
      relJsonSerializer.serialize(rexNode, rowType, java.util.Collections.emptyMap())
    } catch {
      case e: Exception =>
        throw new IllegalStateException(s"Failed to serialize RexNode: $rexNode", e)
    }
  }

  /**
   * Deserialize RexNode using RelJsonSerializer. Extracts the RexNode from the deserialized map.
   */
  private def deserialize(struct: String): RexNode = {
    try {
      val resultMap = relJsonSerializer.deserialize(struct)
      resultMap.get(RelJsonSerializer.EXPR).asInstanceOf[RexNode]
    } catch {
      case e: Exception =>
        throw new IllegalStateException(s"Failed to deserialize RexNode: $struct", e)
    }
  }

  @throws(classOf[java.io.IOException])
  private def writeObject(out: java.io.ObjectOutputStream): Unit = {
    serializedData = serialize(_rexNode)
    out.defaultWriteObject()
  }

  @throws(classOf[java.io.IOException])
  @throws(classOf[ClassNotFoundException])
  private def readObject(in: java.io.ObjectInputStream): Unit = {
    in.defaultReadObject()
    _rexNode = deserialize(serializedData)
  }

  override def toString: String = s"UnifiedFunction(${_rexNode}(${children.mkString(",")}))"

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }
}
