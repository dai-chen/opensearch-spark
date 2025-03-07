/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.function.topksketch

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream, ObjectInputStream, ObjectOutputStream}

import scala.collection.mutable

class AccurateSketch[T](k: Int) extends TopKSketch[T] with Serializable {

  override val name: String = "accurate"

  // Internal map to track item counts
  private val itemCounts = mutable.HashMap.empty[T, Long]

  // Min-heap to maintain the Top K items
  // private implicit val ordering: Ordering[(T, Long)] = Ordering.by(_._2) // Min-Heap based on count
  // private val topKHeap = mutable.PriorityQueue.empty[(T, Long)]

  override def update(item: T): Unit = {
    update(item, 1)
  }

  override def update(item: T, increment: Long): Unit = {
    // Increment the count for the item
    itemCounts.update(item, itemCounts.getOrElse(item, 0L) + increment)
  }

  override def merge(other: TopKSketch[T]): Unit = {
    other match {
      case accurate: AccurateSketch[T] =>
        // Merge item counts from the other sketch
        accurate.itemCounts.foreach { case (item, count) =>
          itemCounts.update(item, itemCounts.getOrElse(item, 0L) + count)
        }
      case _ => throw new IllegalArgumentException("Cannot merge with incompatible sketch")
    }
  }

  override def getTopK: Seq[(T, Long)] = {
    // Rebuild the heap with the latest counts
    val minHeap = mutable.PriorityQueue.empty[(T, Long)](Ordering.by(-_._2)) // Min-heap on count

    for ((item, count) <- itemCounts) {
      if (minHeap.size < k) {
        // If heap is not full, just enqueue
        minHeap.enqueue((item, count))
      } else if (count > minHeap.head._2) {
        // If count is greater than the smallest in heap, replace it
        minHeap.dequeue() // Remove smallest
        minHeap.enqueue((item, count))
      }
    }

    // Convert heap to descending order (top-K highest first)
    minHeap.toSeq.sortBy(-_._2)
  }

  override def serialize(): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(this)
    oos.close()
    bos.toByteArray
  }

  override def deserialize(bytes: Array[Byte]): TopKSketch[T] = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)
    val obj = ois.readObject().asInstanceOf[AccurateSketch[T]]
    ois.close()
    obj
  }
}
