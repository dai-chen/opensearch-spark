/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query.api

import scala.collection.JavaConverters._

import org.apache.calcite.{DataContext, DataContexts}
import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.jdbc.JavaTypeFactoryImpl
import org.apache.calcite.linq4j.QueryProvider
import org.apache.calcite.plan.{RelOptCluster, RelOptPlanner}
import org.apache.calcite.plan.volcano.VolcanoPlanner
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeField}
import org.apache.calcite.rex.{RexBuilder, RexCall, RexExecutable, RexExecutorImpl, RexInputRef, RexNode}
import org.apache.calcite.schema.SchemaPlus
import org.opensearch.sql.opensearch.storage.serde.RelJsonSerializer

/**
 * A serializable wrapper for RexNode that delegates serialization/deserialization to OpenSearch's
 * RelJsonSerializer for safe distribution across executors.
 *
 * RelJsonSerializer handles all the complexity of Calcite's JSON-based serialization, including
 * proper InputTranslator usage for RexInputRef reconstruction.
 */
class SerializableRexNode(@transient private var _rexNode: RexNode) extends Serializable {

  require(_rexNode != null, "RexNode must not be null")
  require(
    _rexNode.isInstanceOf[RexCall],
    s"SerializableRexNode expects a RexCall but received: ${_rexNode.getClass.getName}")

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
  @transient private lazy val rowType: RelDataType = extractInputSchema(asRexCall(_rexNode))

  def getRexNode: RexNode = _rexNode

  def evaluate(inputs: Seq[Any]): Any = {
    val rexCall = asRexCall(_rexNode)
    val dataContext = createDataContext(inputs, rowType)
    val executor = RexExecutorImpl.getExecutable(
      rexBuilder,
      java.util.Collections.singletonList(rexCall),
      rowType)
    executor.setDataContext(dataContext)
    val result = executor.execute()
    if (result == null || result.isEmpty) null else result(0)
  }

  private def createDataContext(inputs: Seq[Any], inputRowType: RelDataType): DataContext = {
    val fields = inputRowType.getFieldList.asScala.zipWithIndex
    val valuesArray: Array[AnyRef] = inputs
      .map {
        case null => null
        case v => v.asInstanceOf[AnyRef]
      }
      .toArray[AnyRef]

    val fieldIndexByName = fields.map { case (field: RelDataTypeField, idx) =>
      field.getName -> idx
    }.toMap

    val baseContext = DataContexts.of((name: String) =>
      name match {
        case "inputRecord" => valuesArray
        case DataContext.Variable.UTC_TIMESTAMP.camelName =>
          java.lang.Long.valueOf(System.currentTimeMillis())
        case fieldName =>
          fieldIndexByName.get(fieldName) match {
            case Some(idx) if idx < inputs.length =>
              inputs(idx).asInstanceOf[AnyRef]
            case _ => null
          }
      })

    new DataContext {
      override def getRootSchema: SchemaPlus = null

      override def getTypeFactory: JavaTypeFactory = typeFactory

      override def getQueryProvider: QueryProvider = null

      override def get(name: String): AnyRef =
        baseContext.get(name).asInstanceOf[AnyRef]
    }
  }

  /**
   * Extract input schema (RelDataType) from RexNode by collecting all RexInputRef types.
   */
  private def extractInputSchema(rexCall: RexCall): RelDataType = {
    import scala.collection.mutable

    val inputRefs = mutable.Map[Int, RelDataType]()

    def collectInputRefs(node: RexNode): Unit = node match {
      case inputRef: RexInputRef =>
        inputRefs(inputRef.getIndex) = inputRef.getType
      case call: RexCall =>
        call.getOperands.asScala.foreach(collectInputRefs)
      case _ => // Other node types don't have inputs
    }

    collectInputRefs(rexCall)

    // Build RelDataType with collected input types
    if (inputRefs.isEmpty) {
      typeFactory.createStructType(
        java.util.Collections.emptyList(),
        java.util.Collections.emptyList())
    } else {
      val sortedInputs = inputRefs.toSeq.sortBy(_._1)
      val types = sortedInputs.map(_._2).asJava
      val names = sortedInputs.map(idx => s"_${idx._1}").asJava
      typeFactory.createStructType(types, names)
    }
  }

  private def asRexCall(node: RexNode): RexCall =
    if (node == null) {
      throw new IllegalArgumentException("SerializableRexNode requires a non-null RexCall")
    } else {
      node match {
        case call: RexCall => call
        case other =>
          throw new IllegalArgumentException(
            s"SerializableRexNode expects a RexCall but received: ${other.getClass.getName}")
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
    _rexNode = asRexCall(deserialize(serializedData))
  }
}
