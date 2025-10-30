/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query.api

import java.util.Objects

import scala.collection.JavaConverters._

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.{RexCall, RexNode}

/**
 * Adapter that turns a Calcite [[RexNode]] backed PPL function into a reusable
 * [[UnifiedFunction]].
 *
 * The adapter stores the RexNode via SerializableRexNode wrapper for runtime evaluation.
 * SerializableRexNode handles serialization internally to enable safe distributed execution.
 */
class UnifiedFunctionCalciteAdapter(private val serializableRexNode: SerializableRexNode)
    extends UnifiedFunction
    with Serializable {

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
      serializableRexNode.evaluate(inputs)
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
