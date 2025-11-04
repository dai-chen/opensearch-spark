/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query.wrapper

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.opensearch.flint.spark.query.calcite.CalciteTypeConverter
import org.opensearch.sql.calcite.udf.UserDefinedAggFunction

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.TypedImperativeAggregate
import org.apache.spark.sql.types.{DataType, StringType}

/**
 * Spark aggregate expression wrapper for Calcite UDAF functions.
 *
 * Uses TypedImperativeAggregate to wrap UserDefinedAggFunction instances, providing a bridge
 * between Calcite's UDAF pattern and Spark's aggregation framework.
 *
 * The merge strategy is provided as a function parameter, allowing each UDAF to define its own
 * merge logic without requiring subclasses.
 *
 * @param udafInstance
 *   The Calcite UDAF instance to wrap
 * @param mergeFn
 *   Function to merge two accumulator buffers. Takes (destination, source) and returns merged
 *   accumulator. For UDAFs that don't support distributed aggregation, this should throw
 *   UnsupportedOperationException.
 * @param children
 *   The input expressions to the aggregate function
 * @param mutableAggBufferOffset
 *   Offset in the mutable aggregation buffer
 * @param inputAggBufferOffset
 *   Offset in the input aggregation buffer
 * @tparam ACC
 *   The accumulator type from the UDAF, must extend UserDefinedAggFunction.Accumulator
 */
case class UnifiedAggregateSparkWrapper[ACC <: UserDefinedAggFunction.Accumulator](
    udafClass: Class[_ <: UserDefinedAggFunction[ACC]],
    mergeFn: (ACC, ACC) => ACC,
    override val children: Seq[Expression],
    override val mutableAggBufferOffset: Int = 0,
    override val inputAggBufferOffset: Int = 0)
    extends TypedImperativeAggregate[ACC]
    with Serializable {

  // Lazily create UDAF instance on each executor (not serialized)
  @transient private lazy val udafInstance: UserDefinedAggFunction[ACC] =
    udafClass.newInstance()

  /**
   * Create a fresh accumulator buffer for aggregation.
   *
   * @return
   *   A new accumulator instance initialized by the UDAF
   */
  override def createAggregationBuffer(): ACC = {
    udafInstance.init()
  }

  /**
   * Update the accumulator buffer with a new input row.
   *
   * @param buffer
   *   The current accumulator state
   * @param input
   *   The input row to process
   * @return
   *   The updated accumulator
   */
  override def update(buffer: ACC, input: InternalRow): ACC = {
    // Evaluate child expressions to get input values
    val sparkValues = children.map(_.eval(input))

    // Convert Spark internal values to Calcite values
    val calciteInputs = sparkValues.zip(children).map { case (value, expr) =>
      CalciteTypeConverter.sparkToCalciteValue(value, expr.dataType)
    }

    // Call UDAF add method with converted inputs
    // Cast to Array[Object] to match Java varargs signature
    udafInstance.add(buffer, calciteInputs.map(_.asInstanceOf[Object]).toArray: _*)
  }

  /**
   * Merge two accumulator buffers for distributed aggregation.
   *
   * Delegates to the merge function provided at construction time.
   *
   * @param buffer
   *   The destination accumulator buffer
   * @param input
   *   The source accumulator buffer to merge
   * @return
   *   The merged accumulator
   */
  override def merge(buffer: ACC, input: ACC): ACC = mergeFn(buffer, input)

  /**
   * Extract the final aggregation result from the accumulator buffer.
   *
   * @param buffer
   *   The final accumulator state
   * @return
   *   The aggregation result converted to Spark type
   */
  override def eval(buffer: ACC): Any = {
    // Get result from UDAF
    val calciteResult = udafInstance.result(buffer)

    // Convert Calcite result to Spark internal value
    CalciteTypeConverter.calciteToSparkValue(calciteResult, dataType)
  }

  /**
   * Serialize the accumulator buffer for shuffle or spill to disk.
   *
   * Extracts the result value from the accumulator and serializes it, rather than serializing the
   * accumulator object itself (which may not be Serializable).
   *
   * @param buffer
   *   The accumulator to serialize
   * @return
   *   Serialized byte array
   */
  override def serialize(buffer: ACC): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    try {
      // Serialize the accumulator's value, not the accumulator itself
      val value = buffer.value()
      oos.writeObject(value)
      bos.toByteArray
    } finally {
      oos.close()
      bos.close()
    }
  }

  /**
   * Deserialize an accumulator buffer from bytes.
   *
   * Creates a fresh accumulator and reconstructs its state from the serialized value.
   *
   * @param bytes
   *   The serialized accumulator
   * @return
   *   The deserialized accumulator
   */
  override def deserialize(bytes: Array[Byte]): ACC = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)
    try {
      val value = ois.readObject()

      // Create fresh accumulator and reconstruct state
      val acc = udafInstance.init()

      // Reconstruct accumulator from serialized value
      // For ValuesAccumulator, value is ArrayList<String>
      value match {
        case list: java.util.ArrayList[_] =>
          import scala.collection.JavaConverters._
          list.asScala.foreach { item =>
            udafInstance.add(acc, item.asInstanceOf[Object])
          }
        case _ => // Handle other types
      }

      acc
    } finally {
      ois.close()
      bis.close()
    }
  }

  /**
   * The data type of the aggregation result.
   *
   * TODO: Infer actual type from SqlAggFunction metadata instead of defaulting to StringType
   *
   * @return
   *   The Spark DataType of the result
   */
  override def dataType: DataType = StringType

  /**
   * Whether the result can be null.
   *
   * @return
   *   true, as aggregate results can generally be null
   */
  override def nullable: Boolean = true

  /**
   * Create a new instance with updated mutable buffer offset.
   *
   * @param newOffset
   *   The new offset value
   * @return
   *   A new instance with the updated offset
   */
  override def withNewMutableAggBufferOffset(newOffset: Int): TypedImperativeAggregate[ACC] = {
    copy(mutableAggBufferOffset = newOffset)
  }

  /**
   * Create a new instance with updated input buffer offset.
   *
   * @param newOffset
   *   The new offset value
   * @return
   *   A new instance with the updated offset
   */
  override def withNewInputAggBufferOffset(newOffset: Int): TypedImperativeAggregate[ACC] = {
    copy(inputAggBufferOffset = newOffset)
  }

  /**
   * Pretty name for display purposes.
   *
   * @return
   *   The simple class name of the UDAF
   */
  override def prettyName: String = udafInstance.getClass.getSimpleName

  /**
   * Create a new instance with updated children.
   *
   * @param newChildren
   *   The new child expressions
   * @return
   *   A new instance with the updated children
   */
  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }
}
