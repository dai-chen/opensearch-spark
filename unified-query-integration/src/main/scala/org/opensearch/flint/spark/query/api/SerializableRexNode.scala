/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query.api

import scala.collection.JavaConverters._

import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.jdbc.JavaTypeFactoryImpl
import org.apache.calcite.plan.{RelOptCluster, RelOptPlanner}
import org.apache.calcite.plan.volcano.VolcanoPlanner
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.{RexBuilder, RexCall, RexInputRef, RexNode}
import org.opensearch.sql.opensearch.storage.serde.RelJsonSerializer

import org.apache.spark.internal.Logging

/**
 * A serializable wrapper for RexNode that delegates serialization/deserialization to OpenSearch's
 * RelJsonSerializer for proper Spark distribution.
 *
 * RelJsonSerializer handles all the complexity of Calcite's JSON-based serialization, including
 * proper InputTranslator usage for RexInputRef reconstruction.
 */
class SerializableRexNode(@transient private var _rexNode: RexNode)
    extends Logging
    with Serializable {

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

  def getRexNode: RexNode = _rexNode

  /**
   * Extract input schema (RelDataType) from RexNode by collecting all RexInputRef types.
   */
  private def extractInputSchema(rexNode: RexNode): RelDataType = {
    import scala.collection.mutable

    val inputRefs = mutable.Map[Int, RelDataType]()

    def collectInputRefs(node: RexNode): Unit = {
      node match {
        case inputRef: RexInputRef =>
          inputRefs(inputRef.getIndex) = inputRef.getType
        case call: RexCall =>
          call.getOperands.asScala.foreach(collectInputRefs)
        case _ => // Other node types don't have inputs
      }
    }

    collectInputRefs(rexNode)

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

  /**
   * Serialize RexNode using RelJsonSerializer. fieldTypes parameter is empty since it's
   * OpenSearch-specific (ExprType).
   */
  private def serialize(rexNode: RexNode): String = {
    try {
      val rowType = extractInputSchema(rexNode)
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
}
