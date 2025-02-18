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
    override val mutableAggBufferOffset: Int = 0,
    override val inputAggBufferOffset: Int = 0)
    extends TypedImperativeAggregate[SpaceSavingSumSketch] {

  override def nullable: Boolean = false

  // Dynamically infer the data type from the key expression
  override def dataType: DataType = ArrayType(
    StructType(
      Seq(
        StructField("value", keyExpr.dataType, nullable = false),
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
    // Convert Top K results to Spark-compatible InternalRow
    val resultArray = buffer.getTopK.map { case (key, sum) =>
      val row = new GenericInternalRow(2)
      row.update(0, convertToDataType(key, keyExpr.dataType))
      row.update(1, sum)
      row
    }

    // Convert the array to ArrayData
    ArrayData.toArrayData(resultArray)
  }

  private def convertToDataType(item: String, targetType: DataType): Any = targetType match {
    case StringType => UTF8String.fromString(item)
    case IntegerType => item.toInt
    case LongType => item.toLong
    case DoubleType => item.toDouble
    case FloatType => item.toFloat
    case BooleanType => item.toBoolean
    case StructType(fields) =>
      // Parse comma-separated fields into a Struct
      val values = item.split(",").map(_.trim)
      val struct = new GenericInternalRow(fields.length)
      fields.zip(values).zipWithIndex.foreach { case ((field, value), index) =>
        struct.update(index, convertToDataType(value, field.dataType))
      }
      struct
    case _ =>
      throw new IllegalArgumentException(s"Unsupported data type: $targetType")
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
