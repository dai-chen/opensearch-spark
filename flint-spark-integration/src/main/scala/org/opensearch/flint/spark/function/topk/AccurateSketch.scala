/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.function.topk

import scala.collection.mutable

class AccurateTopKSketch[T](k: Int) extends TopKSketch[T] {

  // Internal map to track item counts
  private val itemCounts = mutable.HashMap.empty[T, Long]

  // Min-heap to maintain the Top K items
  private implicit val ordering: Ordering[(T, Long)] = Ordering.by(_._2)
  private val topKHeap = mutable.PriorityQueue.empty[(T, Long)]

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
    topKHeap.clear()
    itemCounts.foreach { case (item, count) =>
      topKHeap.enqueue((item, count))
      if (topKHeap.size > k) topKHeap.dequeue() // Maintain only the top K items
    }

    // Return the Top K items sorted by count in descending order
    topKHeap.toSeq.sortBy(-_._2)
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
