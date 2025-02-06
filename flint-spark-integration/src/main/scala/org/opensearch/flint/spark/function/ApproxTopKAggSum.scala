/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.function

import scala.collection.mutable

import org.opensearch.flint.spark.function.topksketch.{SpaceSavingSumSketch, TopKSketch}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.TypedImperativeAggregate
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

case class ApproxTopKAggSum(
    keyExpr: Expression,
    weightExpr: Expression,
    k: Int,
    override val mutableAggBufferOffset: Int = 0,
    override val inputAggBufferOffset: Int = 0)
    extends TypedImperativeAggregate[SpaceSavingSumSketch] {

  override def nullable: Boolean = false

  override def dataType: DataType = ArrayType(
    StructType(
      Seq(
        StructField("value", StringType, nullable = false),
        StructField("sum", DoubleType, nullable = false))))

  override def children: Seq[Expression] = Seq(keyExpr, weightExpr)

  override def createAggregationBuffer(): SpaceSavingSumSketch = {
    new SpaceSavingSumSketch(k)
  }

  override def update(
      buffer: SpaceSavingSumSketch,
      inputRow: InternalRow): SpaceSavingSumSketch = {
    val key = keyExpr.eval(inputRow)
    val weight = weightExpr.eval(inputRow)

    if (key != null && weight != null) {
      val weightValue = weight.asInstanceOf[Number].doubleValue()
      if (!weightValue.isNaN && weightValue >= 0) {
        buffer.update((key.toString, weightValue))
      }
    }

    buffer
  }

  override def merge(
      buffer: SpaceSavingSumSketch,
      input: SpaceSavingSumSketch): SpaceSavingSumSketch = {
    buffer.merge(input)
    buffer
  }

  override def eval(buffer: SpaceSavingSumSketch): Any = {
    // Convert Top K results to Spark-compatible InternalRow and UTF8String
    val resultArray = buffer.getTopK.map { case (key, sum) =>
      InternalRow(UTF8String.fromString(key), sum)
    }

    // Convert the array to ArrayData
    ArrayData.toArrayData(resultArray)
  }

  override def serialize(buffer: SpaceSavingSumSketch): Array[Byte] = {
    buffer.serialize()
  }

  override def deserialize(bytes: Array[Byte]): SpaceSavingSumSketch = {
    new SpaceSavingSumSketch(k).deserialize(bytes)
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): TypedImperativeAggregate[SpaceSavingSumSketch] = {
    copy(keyExpr = newChildren.head, weightExpr = newChildren(1))
  }

  override def withNewMutableAggBufferOffset(
      newOffset: Int): TypedImperativeAggregate[SpaceSavingSumSketch] = {
    copy(mutableAggBufferOffset = newOffset)
  }

  override def withNewInputAggBufferOffset(
      newOffset: Int): TypedImperativeAggregate[SpaceSavingSumSketch] = {
    copy(inputAggBufferOffset = newOffset)
  }
}
