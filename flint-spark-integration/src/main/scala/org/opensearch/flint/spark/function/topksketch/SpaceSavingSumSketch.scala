/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.function.topksketch

import java.nio.ByteBuffer
import java.util.Base64

import scala.collection.mutable

/**
 * Space-Saving Sketch for APPROX_TOP_SUM. Tracks the Top K elements with the highest approximate
 * summed weights.
 */
class SpaceSavingSumSketch(k: Int) {
  private val tracked = 1000

  // Map to store elements and their summed weights
  private val elementSums = mutable.Map.empty[String, Double]

  def update(item: (String, Double)): Unit = {
    val (key, weight) = item

    if (weight.isNaN || weight < 0) {
      throw new IllegalArgumentException("Weight must be non-negative and not NaN")
    }

    if (elementSums.contains(key)) {
      // Increment the weight if the item is already tracked
      elementSums.update(key, elementSums(key) + weight)
    } else if (elementSums.size < tracked) {
      // Add new item if there's space
      elementSums.update(key, weight)
    } else {
      // Replace the item with the smallest weight if full
      val (minItem, minWeight) = elementSums.minBy(_._2)
      elementSums.remove(minItem)
      elementSums.update(
        key,
        weight + minWeight
      ) // Increment by smallest weight during replacement
    }
  }

  def merge(other: SpaceSavingSumSketch): Unit = {
    other match {
      case ssAdapter: SpaceSavingSumSketch =>
        ssAdapter.elementSums.foreach { case (item, sumWeight) =>
          elementSums.update(item, elementSums.getOrElse(item, 0.0) + sumWeight)
        }

      case _ => throw new IllegalArgumentException("Cannot merge with incompatible sketch")
    }
  }

  def getTopK: Seq[(String, Double)] = {
    // Return only the Top K elements sorted by summed weight descending
    elementSums.toSeq.sortBy(-_._2).take(k)
  }

  def serialize(): Array[Byte] = {
    // Serialize the elementSums map to a string in a safer way
    val sumsString = elementSums
      .map { case (item, sum) =>
        val encodedKey = Base64.getEncoder.encodeToString(item.getBytes("UTF-8"))
        s"$encodedKey:$sum"
      }
      .mkString("\n")

    sumsString.getBytes("UTF-8")
  }

  def deserialize(bytes: Array[Byte]): SpaceSavingSumSketch = {
    val sumsString = new String(bytes, "UTF-8")

    // Create a new sketch and restore state with defensive parsing
    val sketch = new SpaceSavingSumSketch(k)

    sumsString.split("\n").foreach { entry =>
      val parts = entry.split(":")
      if (parts.length == 2) {
        try {
          val decodedKey = new String(Base64.getDecoder.decode(parts(0)), "UTF-8")
          val sumValue = parts(1).toDouble
          sketch.elementSums.update(decodedKey, sumValue)
        } catch {
          case _: IllegalArgumentException => // Ignore corrupted lines
        }
      }
    }

    sketch
  }
}
