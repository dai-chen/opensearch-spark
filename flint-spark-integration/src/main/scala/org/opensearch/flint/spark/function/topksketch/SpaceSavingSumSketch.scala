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
  private val elementSums =
    mutable.Map.empty[String, Long] // <--- Changed Double to Long only here

  def update(item: (String, Long)): Unit = {
    val (key, weight) = item
    if (weight < 0) throw new IllegalArgumentException("Weight must be non-negative")
    if (elementSums.contains(key)) {
      elementSums.update(key, elementSums(key) + weight)
    } else if (elementSums.size < tracked) {
      elementSums.update(key, weight)
    } else {
      val (minItem, minWeight) = elementSums.minBy(_._2)
      elementSums.remove(minItem)
      elementSums.update(key, weight + minWeight)
    }
  }

  def merge(other: SpaceSavingSumSketch): Unit = {
    other.elementSums.foreach { case (item, sumWeight) =>
      elementSums.update(item, elementSums.getOrElse(item, 0L) + sumWeight)
    }
  }

  def getTopK: Seq[(String, Long)] =
    elementSums.toSeq.sortBy(-_._2).take(k) // <--- Return type Long

  def serialize(): Array[Byte] = {
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
    val sketch = new SpaceSavingSumSketch(k)
    sumsString.split("\n").foreach { entry =>
      val parts = entry.split(":")
      if (parts.length == 2) {
        try {
          val decodedKey = new String(Base64.getDecoder.decode(parts(0)), "UTF-8")
          val sumValue = parts(1).toLong // <--- Changed to Long
          sketch.elementSums.update(decodedKey, sumValue)
        } catch {
          case _: IllegalArgumentException => // Ignore corrupted lines
        }
      }
    }
    sketch
  }
}
