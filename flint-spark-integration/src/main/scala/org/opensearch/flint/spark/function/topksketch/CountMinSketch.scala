/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.flint.spark.function.topksketch

import java.nio.ByteBuffer

import scala.collection.mutable

import org.apache.spark.util.sketch.{CountMinSketch => SparkCountMinSketch}

/**
 * Count-Min Sketch adapter that implements TopKSketch using Spark's CMS.
 */
class CountMinSketch(k: Int, width: Int = 1024, depth: Int = 5, seed: Int = 42)
    extends TopKSketch[String] {

  // Internal CMS
  private val cms = SparkCountMinSketch.create(width, depth, seed)

  // Min-heap to store up to K items
  private implicit val ordering: Ordering[(String, Long)] = Ordering.by(_._2)
  private val topKHeap = mutable.PriorityQueue.empty[(String, Long)](ordering.reverse)

  override def update(item: String): Unit = {
    // Update CMS
    cms.add(item, 1)
    val estimatedCount = cms.estimateCount(item)

    // If item is already in the heap, update its count
    if (topKHeap.exists(_._1 == item)) {
      // Efficiently update or replace item in the heap
      val currentItems = topKHeap.dequeueAll
      val filteredItems = currentItems.filterNot(_._1 == item)

      // Rebuild the heap with filtered items and the updated item
      topKHeap ++= filteredItems
      topKHeap.enqueue((item, estimatedCount))

      // Ensure the heap size is bounded by K
      if (topKHeap.size > k) topKHeap.dequeue()
    } else if (topKHeap.size < k || estimatedCount > topKHeap.head._2) {
      // If the heap has space or the item is more frequent than the smallest, add it
      if (topKHeap.size >= k) topKHeap.dequeue()
      topKHeap.enqueue((item, estimatedCount))
    }
  }

  override def merge(other: TopKSketch[String]): Unit = {
    other match {
      case cmsAdapter: CountMinSketch =>
        // Merge CMS
        cms.mergeInPlace(cmsAdapter.cms)

        // Merge Top K items from the other sketch
        cmsAdapter.getTopK.foreach { case (item, count) =>
          update(item)
        }

      case _ => throw new IllegalArgumentException("Cannot merge with incompatible sketch")
    }
  }

  override def getTopK: Seq[(String, Long)] = {
    // Return Top K items sorted by count descending
    topKHeap.clone().dequeueAll.sortBy(-_._2)
  }

  override def serialize(): Array[Byte] = {
    // Serialize CMS
    val cmsBytes = cms.toByteArray

    // Serialize Top K heap as a string
    val topKCountsString = topKHeap.map { case (item, count) => s"$item:$count" }.mkString(",")
    val topKCountsBytes = topKCountsString.getBytes("UTF-8")

    // Combine CMS and Top K data
    val buffer = ByteBuffer.allocate(4 + cmsBytes.length + topKCountsBytes.length)
    buffer.putInt(cmsBytes.length)
    buffer.put(cmsBytes)
    buffer.put(topKCountsBytes)
    buffer.array()
  }

  override def deserialize(bytes: Array[Byte]): TopKSketch[String] = {
    val buffer = ByteBuffer.wrap(bytes)

    // Read and deserialize CMS
    val cmsLength = buffer.getInt()
    val cmsBytes = new Array[Byte](cmsLength)
    buffer.get(cmsBytes)
    val deserializedCMS = SparkCountMinSketch.readFrom(cmsBytes)

    // Deserialize Top K items
    val topKCountsBytes = new Array[Byte](buffer.remaining())
    buffer.get(topKCountsBytes)
    val topKCountsString = new String(topKCountsBytes, "UTF-8")

    // Restore sketch
    val sketch = new CountMinSketch(k)
    sketch.cms.mergeInPlace(deserializedCMS)
    topKCountsString.split(",").foreach { entry =>
      val Array(item, count) = entry.split(":")
      sketch.topKHeap.enqueue((item, count.toLong))
    }
    sketch
  }
}
