/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.function

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.apache.datasketches.frequencies.{ErrorType, ItemsSketch}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.types._

case class ApproxTopKAgg(
    child: Expression,
    k: Int,
    override val mutableAggBufferOffset: Int = 0,
    override val inputAggBufferOffset: Int = 0)
    extends TypedImperativeAggregate[ItemsSketch[String]] {

  def this(child: Expression, k: Int) = this(child, k, 0, 0)

  override def nullable: Boolean = false

  override def dataType: DataType = ArrayType(
    StructType(Seq(StructField("value", StringType), StructField("count", StringType))))

  override def children: Seq[Expression] = Seq(child)

  override def createAggregationBuffer(): ItemsSketch[String] = {
    new ItemsSketch[String](k)
  }

  override def update(buffer: ItemsSketch[String], inputRow: InternalRow): ItemsSketch[String] = {
    val value = child.eval(inputRow)
    if (value != null) {
      buffer.update(value.toString)
    }
    buffer
  }

  override def merge(
      buffer: ItemsSketch[String],
      input: ItemsSketch[String]): ItemsSketch[String] = {
    buffer.merge(input)
    buffer
  }

  override def eval(buffer: ItemsSketch[String]): Any = {
    buffer
      .getFrequentItems(ErrorType.NO_FALSE_NEGATIVES)
      .map(item => InternalRow(item.getItem, item.getEstimate.toString))
      .toSeq
  }

  override def serialize(buffer: ItemsSketch[String]): Array[Byte] = {
    val byteStream = new ByteArrayOutputStream()
    val objStream = new ObjectOutputStream(byteStream)
    objStream.writeObject(buffer)
    objStream.close()
    byteStream.toByteArray
  }

  override def deserialize(bytes: Array[Byte]): ItemsSketch[String] = {
    val byteStream = new ByteArrayInputStream(bytes)
    val objStream = new ObjectInputStream(byteStream)
    val sketch = objStream.readObject().asInstanceOf[ItemsSketch[String]]
    objStream.close()
    sketch
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression =
    copy(child = newChildren.head)

  override def withNewMutableAggBufferOffset(newOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newOffset)

  override def withNewInputAggBufferOffset(newOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newOffset)
}
