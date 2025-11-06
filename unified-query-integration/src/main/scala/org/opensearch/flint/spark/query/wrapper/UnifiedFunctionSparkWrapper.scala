/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query.wrapper

import java.util.Collections

import scala.collection.JavaConverters._

import org.apache.calcite.{DataContext, DataContexts}
import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.plan.{RelOptCluster, RelOptPlanner}
import org.apache.calcite.plan.volcano.VolcanoPlanner
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.{RexBuilder, RexCall, RexExecutable, RexInputRef, RexNode}
import org.opensearch.flint.spark.query.calcite.CalciteTypeConverter
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory
import org.opensearch.sql.data.`type`.ExprType
import org.opensearch.sql.opensearch.storage.script.CalciteScriptEngine
import org.opensearch.sql.opensearch.storage.serde.RelJsonSerializer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, NonSQLExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.DataType

/**
 * Spark expression wrapper that directly evaluates Calcite RexExecutable. Avoids Guava
 * deserialization issues by accepting pre-compiled RexExecutable from repository.
 */
case class UnifiedFunctionSparkWrapper(
    @transient private var rexExecutor: RexExecutable,
    private val sparkDataType: DataType,
    private val isNullable: Boolean,
    override val children: Seq[Expression])
    extends Expression
    with CodegenFallback
    with NonSQLExpression
    with Logging {

  // Serialized form - store generated code string instead of RexNode/RexExecutable
  private var serializedCode: String = _

  override def dataType: DataType = sparkDataType

  override def nullable: Boolean = isNullable

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
    // Create map with field names as keys: "_0", "_1", "_2", etc.
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

  override def toString: String = s"UnifiedFunction($sparkDataType(${children.mkString(",")}))"

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }
}
