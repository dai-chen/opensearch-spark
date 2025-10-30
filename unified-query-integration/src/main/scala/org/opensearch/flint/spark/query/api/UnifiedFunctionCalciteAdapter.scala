/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query.api

import java.util.Objects

import scala.collection.JavaConverters._

import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.jdbc.JavaTypeFactoryImpl
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.rex.{RexBuilder, RexCall, RexInputRef, RexInterpreter, RexNode}

/**
 * Adapter that turns a Calcite [[RexNode]] backed PPL function into a reusable
 * [[UnifiedFunction]].
 *
 * The adapter stores the RexNode via SerializableRexNode wrapper for runtime evaluation.
 * SerializableRexNode handles serialization internally to enable Spark distribution.
 */
class UnifiedFunctionCalciteAdapter(private val serializableRexNode: SerializableRexNode)
    extends UnifiedFunction
    with Serializable {

  @transient private lazy val typeFactory: JavaTypeFactory = new JavaTypeFactoryImpl()

  @transient private lazy val rexBuilder: RexBuilder = new RexBuilder(typeFactory)

  override def returnType: RelDataType = serializableRexNode.getRexNode.getType

  override def nullable: Boolean = serializableRexNode.getRexNode.getType.isNullable

  override def inputTypes: Seq[RelDataType] = {
    serializableRexNode.getRexNode match {
      case call: RexCall =>
        call.getOperands.asScala.map(_.getType).toSeq
      case _ =>
        Seq.empty
    }
  }

  override def eval(inputs: Seq[Any]): Any = {
    try {
      // Build a map from actual RexInputRef nodes in the tree to their values
      val rexNode = serializableRexNode.getRexNode
      val inputMap = new java.util.HashMap[RexNode, Comparable[_]]()

      // Collect all RexInputRef nodes from the RexNode tree
      val inputRefs = new java.util.ArrayList[RexInputRef]()
      def collectInputRefs(node: RexNode): Unit = {
        node match {
          case inputRef: RexInputRef =>
            inputRefs.add(inputRef)
          case call: RexCall =>
            call.getOperands.asScala.foreach(collectInputRefs)
          case _ => // Other node types
        }
      }
      collectInputRefs(rexNode)

      // Bind the actual RexInputRef objects to their values
      inputRefs.asScala.foreach { inputRef =>
        val index = inputRef.getIndex
        if (index < inputs.size) {
          val value = inputs(index)
          // Convert value to Comparable, handling nulls
          val comparableValue = if (value == null) {
            null
          } else if (value.isInstanceOf[Comparable[_]]) {
            value.asInstanceOf[Comparable[_]]
          } else {
            // Wrap non-comparable values
            value.asInstanceOf[Comparable[_]]
          }
          inputMap.put(inputRef, comparableValue)
        }
      }

      RexInterpreter.evaluate(rexNode, inputMap)
    } catch {
      case e: Exception =>
        throw new RuntimeException(s"Failed to evaluate Calcite expression: ${e.getMessage}", e)
    }
  }
}

object UnifiedFunctionCalciteAdapter {

  def apply(rexNode: RexNode): UnifiedFunctionCalciteAdapter = {
    Objects.requireNonNull(rexNode, "rexNode")
    new UnifiedFunctionCalciteAdapter(new SerializableRexNode(rexNode))
  }
}
