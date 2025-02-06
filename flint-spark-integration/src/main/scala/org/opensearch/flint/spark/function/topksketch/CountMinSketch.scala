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

  // Internal CMS and a map to track exact Top K counts
  private val cms = SparkCountMinSketch.create(width, depth, seed)
  private val topKCounts = mutable.Map.empty[String, Long]

  override def update(item: String): Unit = {
    cms.add(item, 1)
    topKCounts.update(item, topKCounts.getOrElse(item, 0L) + 1)
  }

  override def merge(other: TopKSketch[String]): Unit = {
    other match {
      case cmsAdapter: CountMinSketch =>
        cms.mergeInPlace(cmsAdapter.cms)
        cmsAdapter.topKCounts.foreach { case (item, count) =>
          topKCounts.update(item, topKCounts.getOrElse(item, 0L) + count)
        }

      case _ => throw new IllegalArgumentException("Cannot merge with incompatible sketch")
    }
  }

  override def getTopK: Seq[(String, Long)] = {
    topKCounts.toSeq.sortBy(-_._2).take(k)
  }

  override def serialize(): Array[Byte] = {
    // Serialize CMS
    val cmsBytes = cms.toByteArray

    // Serialize topKCounts separately
    val topKCountsString = topKCounts.map { case (item, count) => s"$item:$count" }.mkString(",")
    val topKCountsBytes = topKCountsString.getBytes("UTF-8")

    // Combine CMS and topKCounts, but keep the CMS format intact
    val buffer = ByteBuffer.allocate(4 + cmsBytes.length + topKCountsBytes.length)
    buffer.putInt(cmsBytes.length) // Length of CMS
    buffer.put(cmsBytes) // CMS data
    buffer.put(topKCountsBytes) // TopKCounts data
    buffer.array()
  }

  override def deserialize(bytes: Array[Byte]): TopKSketch[String] = {
    val buffer = ByteBuffer.wrap(bytes)

    // Read CMS length and extract CMS bytes
    val cmsLength = buffer.getInt()
    val cmsBytes = new Array[Byte](cmsLength)
    buffer.get(cmsBytes)

    // Deserialize CMS without extra data interfering
    val deserializedCMS = SparkCountMinSketch.readFrom(cmsBytes)

    // Extract remaining bytes for topKCounts
    val topKCountsBytes = new Array[Byte](buffer.remaining())
    buffer.get(topKCountsBytes)
    val topKCountsString = new String(topKCountsBytes, "UTF-8")

    // Restore topKCounts
    val sketch = new CountMinSketch(k)
    sketch.cms.mergeInPlace(deserializedCMS)
    topKCountsString.split(",").foreach { entry =>
      val Array(item, count) = entry.split(":")
      sketch.topKCounts.update(item, count.toLong)
    }
    sketch
  }
}
