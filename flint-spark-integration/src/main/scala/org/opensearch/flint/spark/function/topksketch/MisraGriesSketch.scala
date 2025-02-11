/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.function.topksketch

import org.apache.datasketches.ArrayOfStringsSerDe
import org.apache.datasketches.frequencies.{ErrorType, ItemsSketch}
import org.apache.datasketches.memory.Memory

class MisraGriesSketch(k: Int) extends TopKSketch[String] {

  private val sketch = new ItemsSketch[String](nextPowerOfTwo(tracked))
  private val serDe = new ArrayOfStringsSerDe()

  override def update(item: String): Unit = {
    sketch.update(item)
  }

  override def merge(other: TopKSketch[String]): Unit = {
    other match {
      case mg: MisraGriesSketch => sketch.merge(mg.sketch)
      case _ =>
        throw new IllegalArgumentException("Cannot merge with incompatible sketch")
    }
  }

  override def getTopK: Seq[(String, Long)] = {
    sketch
      .getFrequentItems(ErrorType.NO_FALSE_POSITIVES)
      .map(item => (item.getItem, item.getEstimate))
      .toSeq
      .sortBy(-_._2) // Sort by frequency descending
      .take(k) // Limit to top K results
  }

  override def serialize(): Array[Byte] = {
    sketch.toByteArray(serDe)
  }

  override def deserialize(bytes: Array[Byte]): TopKSketch[String] = {
    val newSketch = new MisraGriesSketch(k)
    newSketch.sketch.merge(ItemsSketch.getInstance(Memory.wrap(bytes), serDe))
    newSketch
  }

  private def nextPowerOfTwo(x: Int): Int = {
    if (x <= 0) 1
    else 1 << (32 - Integer.numberOfLeadingZeros(x - 1))
  }
}
