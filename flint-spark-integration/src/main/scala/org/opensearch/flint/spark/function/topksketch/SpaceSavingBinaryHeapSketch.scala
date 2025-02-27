/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.function.topksketch

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import scala.collection.mutable
import scala.collection.mutable.PriorityQueue

/**
 * Heap-Based Space-Saving Sketch that efficiently tracks Top K elements. Uses a HashMap for fast
 * lookups and a Min-Heap (PriorityQueue) for maintaining order.
 */
class SpaceSavingBinaryHeapSketch(k: Int, tracked: Int)
    extends TopKSketch[String]
    with Serializable {

  // HashMap to store elements and their counts
  private val elementCounts = mutable.Map.empty[String, Long]

  // Min-Heap to maintain the Top K order
  private val minHeap: PriorityQueue[(String, Long)] =
    PriorityQueue.empty(Ordering.by[(String, Long), Long](_._2).reverse) // Min-Heap

  override def update(item: String): Unit = {
    update(item, 1)
  }

  def update(item: String, increment: Long): Unit = {
    if (elementCounts.contains(item)) {
      // **FIX 1: Remove previous entry from heap before updating**
      minHeap.dequeueAll.filterNot(_._1 == item).foreach { case (item, count) =>
        minHeap.enqueue((item, count))
      }

      // Increment count in HashMap
      elementCounts.update(item, elementCounts(item) + increment)
    } else if (elementCounts.size < tracked) {
      // Add new item if there's space
      elementCounts.update(item, increment)
    } else {
      // **FIX 2: Properly remove the smallest element from both heap and hashmap**
      val (minItem, minCount) = minHeap.dequeue() // Remove smallest count from heap
      elementCounts.remove(minItem) // Remove from HashMap

      // Add new item with updated count
      elementCounts.update(item, minCount + increment)
    }

    // **Ensure Heap is in sync with HashMap**
    minHeap.enqueue((item, elementCounts(item)))

    // **Ensure Heap size stays within K**
    while (minHeap.size > k) minHeap.dequeue()
  }

  override def merge(other: TopKSketch[String]): Unit = {
    other match {
      case otherSketch: SpaceSavingBinaryHeapSketch =>
        otherSketch.elementCounts.foreach { case (item, count) =>
          update(item, count)
        }

      case _ => throw new IllegalArgumentException("Cannot merge with incompatible sketch")
    }
  }

  override def getTopK: Seq[(String, Long)] = {
    // Extract top K elements from heap in descending order
    minHeap.clone().dequeueAll.reverse
  }

  override def serialize(): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(this)
    oos.close()
    bos.toByteArray
  }

  override def deserialize(bytes: Array[Byte]): TopKSketch[String] = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)
    val obj = ois.readObject().asInstanceOf[SpaceSavingBinaryHeapSketch]
    ois.close()
    obj
  }
}
