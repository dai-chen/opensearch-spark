/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.function.topk

import org.apache.datasketches.ArrayOfStringsSerDe
import org.apache.datasketches.frequencies.{ErrorType, ItemsSketch}
import org.apache.datasketches.memory.Memory

class MisraGriesSketch(k: Int) extends TopKSketch[String] {

  private val sketch = new ItemsSketch[String](nextPowerOfTwo(k))
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
      .getFrequentItems(ErrorType.NO_FALSE_NEGATIVES)
      .map(item => (item.getItem, item.getEstimate))
      .toSeq
      .sortBy(-_._2) // Sort by frequency descending
      .take(k) // Limit to top K results
  }

  override def serialize(): Array[Byte] = {
    sketch.toByteArray(serDe)
  }

  override def deserialize(bytes: Array[Byte]): TopKSketch[String] = {
    val newSketch = new ItemsSketch[String](nextPowerOfTwo(k))
    newSketch.merge(ItemsSketch.getInstance(Memory.wrap(bytes), serDe))
    new MisraGriesSketch(k).withSketch(newSketch)
  }

  private def withSketch(newSketch: ItemsSketch[String]): MisraGriesSketch = {
    val newInstance = new MisraGriesSketch(k)
    newInstance.sketch.merge(newSketch)
    newInstance
  }

  private def nextPowerOfTwo(x: Int): Int = {
    if (x <= 0) 1
    else 1 << (32 - Integer.numberOfLeadingZeros(x - 1))
  }
}
