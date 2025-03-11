/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.function.topksketch

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream, Serializable}

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.google.common.collect.MinMaxPriorityQueue

/**
 * Space-Saving Algorithm using Guava's MinMaxPriorityQueue
 */
class SpaceSavingBinaryHeapSketch(k: Int, tracked: Int)
    extends TopKSketch[String]
    with Serializable {

  override val name: String = "space_saving_binary_heap"

  // HashMap to store element counts
  private val elementCounts = mutable.Map.empty[String, Long]

  // Min-Heap using Guava's MinMaxPriorityQueue
  private val minHeap: MinMaxPriorityQueue[(String, Long)] =
    MinMaxPriorityQueue
      .orderedBy[(String, Long)](Ordering.by(_._2)) // Min-Heap Order
      .maximumSize(tracked)
      .create()

  def update(item: String): Unit = update(item, 1)

  def update(item: String, increment: Long): Unit = {
    if (elementCounts.contains(item)) {
      // **Remove old value from heap before updating**
      val oldValue = elementCounts(item)
      minHeap.remove((item, oldValue)) // Guava requires exact object removal!

      // **Update HashMap**
      val newValue = oldValue + increment
      elementCounts.update(item, newValue)

      // **Reinsert updated element into heap**
      minHeap.add((item, newValue))
    } else if (elementCounts.size < tracked) {
      // **Insert new item if space is available**
      elementCounts.update(item, increment)
      minHeap.add((item, increment))
    } else {
      // **Remove smallest element and replace with new**
      val (minItem, minCount) = minHeap.poll() // Remove smallest from heap
      elementCounts.remove(minItem) // Remove from HashMap

      // **Add new element**
      elementCounts.update(item, minCount + increment)
      minHeap.add((item, minCount + increment))
    }
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

  override def getTopK: Seq[(String, Long, Long)] = {
    // Create a snapshot of the heap without modifying it
    minHeap
      .iterator()
      .asScala
      .toSeq
      .map { case (item, cnt) =>
        (item, cnt, 0L)
      }
      .sortBy(-_._2)
      .take(k)
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
