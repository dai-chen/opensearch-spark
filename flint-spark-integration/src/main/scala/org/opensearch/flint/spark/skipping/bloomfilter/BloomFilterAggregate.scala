/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.bloomfilter

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.TypedImperativeAggregate
import org.apache.spark.sql.catalyst.trees.TernaryLike
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.sketch.BloomFilter

/**
 * Copy from Spark to change BF parameters.
 */
case class BloomFilterAggregate(
    child: Expression,
    estimatedNumItemsExpression: Expression,
    numBitsExpression: Expression,
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int)
    extends TypedImperativeAggregate[BloomFilter]
    with TernaryLike[Expression] {

  def this(
      child: Expression,
      estimatedNumItemsExpression: Expression,
      numBitsExpression: Expression) = {
    this(child, estimatedNumItemsExpression, numBitsExpression, 0, 0)
  }

  def this(child: Expression, estimatedNumItemsExpression: Expression) = {
    this(
      child,
      estimatedNumItemsExpression,
      // 1 byte per item.
      Multiply(estimatedNumItemsExpression, Literal(8L)))
  }

  def this(child: Expression) = {
    this(
      child,
      Literal(SQLConf.get.getConf(SQLConf.RUNTIME_BLOOM_FILTER_EXPECTED_NUM_ITEMS)),
      Literal(SQLConf.get.getConf(SQLConf.RUNTIME_BLOOM_FILTER_NUM_BITS)))
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    (first.dataType, second.dataType, third.dataType) match {
      case (_, NullType, _) | (_, _, NullType) =>
        TypeCheckResult.TypeCheckFailure("Null typed values cannot be used as size arguments")
      case (LongType, LongType, LongType) =>
        if (!estimatedNumItemsExpression.foldable) {
          TypeCheckFailure("The estimated number of items provided must be a constant literal")
        } else if (estimatedNumItems <= 0L) {
          TypeCheckFailure(
            "The estimated number of items must be a positive value " +
              s" (current value = $estimatedNumItems)")
        } else if (!numBitsExpression.foldable) {
          TypeCheckFailure("The number of bits provided must be a constant literal")
        } else if (numBits <= 0L) {
          TypeCheckFailure(
            "The number of bits must be a positive value " +
              s" (current value = $numBits)")
        } else {
          require(
            estimatedNumItems <=
              SQLConf.get.getConf(SQLConf.RUNTIME_BLOOM_FILTER_MAX_NUM_ITEMS))
          require(numBits <= SQLConf.get.getConf(SQLConf.RUNTIME_BLOOM_FILTER_MAX_NUM_BITS))
          TypeCheckSuccess
        }
      case _ =>
        TypeCheckResult.TypeCheckFailure(s"Input to function $prettyName should have " +
          s"been a ${LongType.simpleString} value followed with two ${LongType.simpleString} size " +
          s"arguments, but it's [${first.dataType.catalogString}, " +
          s"${second.dataType.catalogString}, ${third.dataType.catalogString}]")
    }
  }
  override def nullable: Boolean = true

  override def dataType: DataType = BinaryType

  override def prettyName: String = "bloom_filter_agg"

  // Mark as lazy so that `estimatedNumItems` is not evaluated during tree transformation.
  private lazy val estimatedNumItems: Long =
    Math.min(
      estimatedNumItemsExpression.eval().asInstanceOf[Number].longValue,
      SQLConf.get.getConf(SQLConf.RUNTIME_BLOOM_FILTER_MAX_NUM_ITEMS))

  // Mark as lazy so that `numBits` is not evaluated during tree transformation.
  private lazy val numBits: Long =
    Math.min(
      numBitsExpression.eval().asInstanceOf[Number].longValue,
      SQLConf.get.getConf(SQLConf.RUNTIME_BLOOM_FILTER_MAX_NUM_BITS))

  override def first: Expression = child

  override def second: Expression = estimatedNumItemsExpression

  override def third: Expression = numBitsExpression

  override protected def withNewChildrenInternal(
      newChild: Expression,
      newEstimatedNumItemsExpression: Expression,
      newNumBitsExpression: Expression): BloomFilterAggregate = {
    copy(
      child = newChild,
      estimatedNumItemsExpression = newEstimatedNumItemsExpression,
      numBitsExpression = newNumBitsExpression)
  }

  override def createAggregationBuffer(): BloomFilter = {
    // BloomFilter.create(estimatedNumItems, numBits)
    // BloomFilter.create(estimatedNumItems)
    AdaptiveBloomFilter.create(estimatedNumItems)
  }

  override def update(buffer: BloomFilter, inputRow: InternalRow): BloomFilter = {
    val value = child.eval(inputRow)
    // Ignore null values.
    if (value == null) {
      return buffer
    }
    buffer.putLong(value.asInstanceOf[Long])
    buffer
  }

  override def merge(buffer: BloomFilter, other: BloomFilter): BloomFilter = {
    buffer.mergeInPlace(other)
  }

  override def eval(buffer: BloomFilter): Any = {
    if (buffer.cardinality() == 0) {
      // There's no set bit in the Bloom filter and hence no not-null value is processed.
      return null
    }

    // Overflown
    /*
    if (buffer.cardinality() > estimatedNumItems) {
      return null
    }
     */

    // Store internal BF when final aggregate result output
    val bf = buffer.asInstanceOf[AdaptiveBloomFilter].bestCandidate()
    val size = (bf.bitSize() / 8) + 8
    require(size <= Integer.MAX_VALUE, s"actual number of bits is too large $size")
    val out = new ByteArrayOutputStream(size.intValue())
    bf.writeTo(out)
    out.close()
    out.toByteArray
  }

  override def withNewMutableAggBufferOffset(newOffset: Int): BloomFilterAggregate =
    copy(mutableAggBufferOffset = newOffset)

  override def withNewInputAggBufferOffset(newOffset: Int): BloomFilterAggregate =
    copy(inputAggBufferOffset = newOffset)

  override def serialize(obj: BloomFilter): Array[Byte] = {
    BloomFilterAggregate.serialize(obj)
  }

  override def deserialize(bytes: Array[Byte]): BloomFilter = {
    BloomFilterAggregate.deserialize(bytes)
  }
}

object BloomFilterAggregate {
  final def serialize(obj: BloomFilter): Array[Byte] = {
    // BloomFilterImpl.writeTo() writes 2 integers (version number and num hash functions), hence
    // the +8
    val size = (obj.bitSize() / 8) + 8 + 4 // write cardinality
    require(size <= Integer.MAX_VALUE, s"actual number of bits is too large $size")
    val out = new ByteArrayOutputStream(size.intValue())
    obj.writeTo(out)
    out.close()
    out.toByteArray
  }

  final def deserialize(bytes: Array[Byte]): BloomFilter = {
    val in = new ByteArrayInputStream(bytes)
    // val bloomFilter = BloomFilter.readFrom(in)
    val bloomFilter = AdaptiveBloomFilter.readFrom(in)
    in.close()
    bloomFilter
  }
}
