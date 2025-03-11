/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.function

import scala.collection.mutable

import org.opensearch.flint.spark.function.topksketch.{SpaceSavingSumSketch, TopKSketch}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow}
import org.apache.spark.sql.catalyst.expressions.aggregate.TypedImperativeAggregate
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, BooleanType, DataType, DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

case class ApproxTopKAggSum(
    keyExpr: Expression,
    weightExpr: Expression,
    k: Int,
    tracked: Int,
    createSketch: (Int, Int) => TopKSketch[String],
    override val mutableAggBufferOffset: Int = 0,
    override val inputAggBufferOffset: Int = 0)
    extends TypedImperativeAggregate[TopKSketch[String]] {

  override def nullable: Boolean = false

  // Changed only DoubleType to LongType here
  override def dataType: DataType = ArrayType(
    StructType(
      Seq(
        StructField("value", keyExpr.dataType, nullable = false),
        StructField("sum", LongType, nullable = false),
        StructField("error", LongType, nullable = false)
      )
    )
  ) // <--- Changed this line only

  override def children: Seq[Expression] = Seq(keyExpr, weightExpr)

  override def createAggregationBuffer(): TopKSketch[String] =
    createSketch(k, tracked)

  override def update(buffer: TopKSketch[String], inputRow: InternalRow): TopKSketch[String] = {
    val key = keyExpr.eval(inputRow)
    val weight = weightExpr.eval(inputRow)
    if (key != null && weight != null) {
      val weightValue = weight.asInstanceOf[Number].longValue() // Changed to Long
      buffer.update(key.toString, weightValue)
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
    val resultArray = buffer.getTopK.map { case (key, sum, error) =>
      val row = new GenericInternalRow(3)
      row.update(0, convertToDataType(key, keyExpr.dataType))
      row.update(1, sum) // sum is now Long
      row.update(2, error)
      row
    }
    ArrayData.toArrayData(resultArray)
  }

  // No change in convertToDataType
  private def convertToDataType(item: String, targetType: DataType): Any = targetType match {
    case StringType => UTF8String.fromString(item)
    case IntegerType => item.toInt
    case LongType => item.toLong
    case DoubleType => item.toDouble
    case FloatType => item.toFloat
    case BooleanType => item.toBoolean
    case StructType(fields) =>
      val values = item.split(",").map(_.trim)
      val struct = new GenericInternalRow(fields.length)
      fields.zip(values).zipWithIndex.foreach { case ((field, value), index) =>
        struct.update(index, convertToDataType(value, field.dataType))
      }
      struct
    case _ => throw new IllegalArgumentException(s"Unsupported data type: $targetType")
  }

  override def serialize(buffer: TopKSketch[String]): Array[Byte] = buffer.serialize()
  override def deserialize(bytes: Array[Byte]): TopKSketch[String] =
    createSketch(k, tracked).deserialize(bytes)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): TypedImperativeAggregate[TopKSketch[String]] =
    copy(keyExpr = newChildren.head, weightExpr = newChildren(1))

  override def withNewMutableAggBufferOffset(
      newOffset: Int): TypedImperativeAggregate[TopKSketch[String]] =
    copy(mutableAggBufferOffset = newOffset)

  override def withNewInputAggBufferOffset(
      newOffset: Int): TypedImperativeAggregate[TopKSketch[String]] =
    copy(inputAggBufferOffset = newOffset)
}
