/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.function

import org.opensearch.flint.spark.function.topksketch.TopKSketch

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow}
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class ApproxTopKAgg(
    child: Expression,
    k: Int,
    createSketch: Int => TopKSketch[String],
    override val mutableAggBufferOffset: Int = 0,
    override val inputAggBufferOffset: Int = 0)
    extends TypedImperativeAggregate[TopKSketch[String]] {

  def this(child: Expression, k: Int, createSketch: Int => TopKSketch[String]) =
    this(child, k, createSketch, 0, 0)

  override def nullable: Boolean = false

  override def dataType: DataType = ArrayType(
    StructType(Seq(StructField("value", StringType), StructField("count", LongType))))

  override def children: Seq[Expression] = Seq(child)

  override def createAggregationBuffer(): TopKSketch[String] = {
    createSketch(k)
  }

  override def update(buffer: TopKSketch[String], inputRow: InternalRow): TopKSketch[String] = {
    val value = child.eval(inputRow)
    if (value != null) {
      buffer.update(value.toString)
    }
    buffer
  }

  override def merge(
      buffer: TopKSketch[String],
      input: TopKSketch[String]): TopKSketch[String] = {
    buffer.merge(input)
    buffer
  }

  override def eval(buffer: TopKSketch[String]): Any = {
    val topKItems = buffer.getTopK.map { case (item, count) =>
      val row = new GenericInternalRow(2)
      row.update(0, UTF8String.fromString(item))
      row.update(1, count)
      row
    }

    // Return as GenericArrayData
    new GenericArrayData(topKItems.toArray)
  }

  override def serialize(buffer: TopKSketch[String]): Array[Byte] = {
    buffer.serialize()
  }

  override def deserialize(bytes: Array[Byte]): TopKSketch[String] = {
    createSketch(k).deserialize(bytes)
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression =
    copy(child = newChildren.head)

  override def withNewMutableAggBufferOffset(newOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newOffset)

  override def withNewInputAggBufferOffset(newOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newOffset)
}
