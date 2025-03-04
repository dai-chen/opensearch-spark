/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.function.topksketch

import scala.collection.mutable

class AccurateTopKSketch[T](k: Int) extends TopKSketch[T] {

  // Internal map to track item counts
  private val itemCounts = mutable.HashMap.empty[T, Long]

  // Min-heap to maintain the Top K items
  // private implicit val ordering: Ordering[(T, Long)] = Ordering.by(_._2) // Min-Heap based on count
  // private val topKHeap = mutable.PriorityQueue.empty[(T, Long)]

  override def update(item: T, weight: Long): Unit = {}

  override def update(item: T): Unit = {
    // Increment the count for the item
    itemCounts.update(item, itemCounts.getOrElse(item, 0L) + 1)
  }

  override def merge(other: TopKSketch[T]): Unit = {
    other match {
      case accurate: AccurateTopKSketch[T] =>
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
    // Serialization for testing purposes (not optimized)
    itemCounts.toArray.map { case (item, count) => s"$item:$count" }.mkString(",").getBytes
  }

  override def deserialize(bytes: Array[Byte]): TopKSketch[T] = {
    val data = new String(bytes).split(",").map { entry =>
      val Array(item, count) = entry.split(":")
      item.asInstanceOf[T] -> count.toLong
    }
    val sketch = new AccurateTopKSketch[T](k)
    sketch.itemCounts ++= data
    sketch
  }
}
