/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.function.topksketch

import java.nio.ByteBuffer
import java.util.Base64

import scala.collection.mutable

/**
 * Space-Saving Sketch that implements TopKSketch. Tracks the Top K elements with the highest
 * estimated counts.
 */
class SpaceSavingSketch(k: Int, tracked: Int) extends TopKSketch[String] {

  override val name: String = "space_saving_hashmap"

  // Map to store elements and their counts
  private val elementCounts = mutable.Map.empty[String, Long]


  override def update(item: String): Unit = {
    update(item, 1)
  }

  override def update(item: String, increment: Long): Unit = {
    if (elementCounts.contains(item)) {
      // Increment the count if the item is already tracked
      elementCounts.update(item, elementCounts(item) + increment)
    } else if (elementCounts.size < tracked) {
      // Add new item if there's space
      elementCounts.update(item, increment)
    } else {
      // Replace the item with the smallest count if full
      val (minItem, minCount) = elementCounts.minBy(_._2)
      elementCounts.remove(minItem)
      elementCounts.update(item, minCount + increment) // Increment the count when replacing
    }
  }

  override def merge(other: TopKSketch[String]): Unit = {
    other match {
      case ssAdapter: SpaceSavingSketch =>
        ssAdapter.elementCounts.foreach { case (item, count) =>
          // elementCounts.update(item, elementCounts.getOrElse(item, 0L) + count)
          update(item, count)
        }

      case _ => throw new IllegalArgumentException("Cannot merge with incompatible sketch")
    }
  }

  override def getTopK: Seq[(String, Long)] = {
    // Return the Top K elements sorted by count descending
    elementCounts.toSeq.sortBy(-_._2).take(k)
  }

  override def serialize(): Array[Byte] = {
    // Serialize the elementCounts map as JSON-like strings
    val countsString = elementCounts
      .map { case (item, count) =>
        val cleanedItem = item.replace("[", "").replace("]", "")
        val encodedKey = Base64.getEncoder.encodeToString(cleanedItem.getBytes("UTF-8"))
        s"$encodedKey:$count"
      }
      .mkString("\n")

    countsString.getBytes("UTF-8")
  }

  override def deserialize(bytes: Array[Byte]): TopKSketch[String] = {
    val countsString = new String(bytes, "UTF-8")

    // Create a new SpaceSavingSketch and restore state with defensive parsing
    val sketch = new SpaceSavingSketch(k, tracked)
    countsString.split("\n").foreach { entry =>
      val parts = entry.split(":")
      if (parts.length == 2) {
        try {
          val decodedKey = new String(Base64.getDecoder.decode(parts(0)), "UTF-8")
          val cleanedKey = decodedKey.replace("[", "").replace("]", "")
          val countValue = parts(1).toLong
          sketch.elementCounts.update(cleanedKey, countValue)
        } catch {
          case _: IllegalArgumentException => // Ignore corrupted lines
          case _: NumberFormatException => // Ignore invalid counts
        }
      }
    }
    sketch
  }
}
