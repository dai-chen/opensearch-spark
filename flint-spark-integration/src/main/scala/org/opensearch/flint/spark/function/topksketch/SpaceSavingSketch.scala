/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.function.topksketch

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import scala.collection.mutable

/**
 * Space-Saving Sketch that implements TopKSketch. Tracks the Top K elements with the highest
 * estimated counts using error bounds.
 */
class SpaceSavingSketch(k: Int, tracked: Int) extends TopKSketch[String] with Serializable {
  override val name: String = "space_saving_hashmap"

  // Map to store elements and their estimated counts with error tracking
  private val elementCounts = mutable.Map.empty[String, Counter]

  override def update(item: String): Unit = update(item, 1)

  override def update(item: String, increment: Long): Unit = {
    elementCounts.get(item) match {
      case Some(counter) =>
        elementCounts.update(item, Counter(counter.frequency + increment, counter.error))
      case None if elementCounts.size < tracked =>
        elementCounts.update(item, Counter(increment, 0L))
      case None =>
        // Replace the item with the smallest count and adjust error
        val (minItem, minCounter) = elementCounts.minBy(_._2.frequency)
        elementCounts.remove(minItem)
        elementCounts.update(
          item,
          Counter(minCounter.frequency + increment, minCounter.frequency))
    }
  }

  override def merge(other: TopKSketch[String]): Unit = {
    other match {
      case ssAdapter: SpaceSavingSketch =>
        val min1 =
          if (this.elementCounts.size == tracked) {
            this.elementCounts.minBy(_._2.frequency)._2.frequency
          } else {
            0L
          }
        val min2 =
          if (ssAdapter.elementCounts.size == tracked) {
            ssAdapter.elementCounts.minBy(_._2.frequency)._2.frequency
          } else {
            0L
          }
        val combined = mutable.Map.empty[String, Counter]

        // Merge common items
        this.elementCounts.foreach { case (item, c1) =>
          ssAdapter.elementCounts.get(item) match {
            case Some(c2) =>
              combined.update(item, Counter(c1.frequency + c2.frequency, c1.error + c2.error))
              ssAdapter.elementCounts.remove(item)
            case None =>
              combined.update(item, Counter(c1.frequency + min2, c1.error + min2))
          }
        }

        // Remaining items in the other sketch
        ssAdapter.elementCounts.foreach { case (item, c2) =>
          combined.update(item, Counter(c2.frequency + min1, c2.error + min1))
        }

        // Keep only the top-K items
        val pruned = combined.toSeq.sortBy(-_._2.frequency).take(tracked)
        elementCounts.clear()
        elementCounts ++= pruned
      case _ =>
        throw new IllegalArgumentException("Cannot merge with incompatible sketch")
    }
  }

  override def getTopK: Seq[(String, Long)] = {
    elementCounts.toSeq.sortBy(-_._2.frequency).take(k).map { case (item, counter) =>
      (item, counter.frequency)
    }
  }

  override def serialize(): Array[Byte] = {
    val byteStream = new ByteArrayOutputStream()
    val objectStream = new ObjectOutputStream(byteStream)

    // Manually write number of entries
    objectStream.writeInt(elementCounts.size)

    // Write each entry: key (UTF), frequency (Long), error (Long)
    elementCounts.foreach { case (key, counter) =>
      objectStream.writeUTF(key)
      objectStream.writeLong(counter.frequency)
      objectStream.writeLong(counter.error)
    }

    objectStream.close()
    byteStream.toByteArray
  }

  override def deserialize(bytes: Array[Byte]): TopKSketch[String] = {
    val byteStream = new ByteArrayInputStream(bytes)
    val objectStream = new ObjectInputStream(byteStream)

    val sketch = new SpaceSavingSketch(k, tracked)

    val numEntries = objectStream.readInt()
    for (_ <- 0 until numEntries) {
      val key = objectStream.readUTF()
      val frequency = objectStream.readLong()
      val error = objectStream.readLong()
      sketch.elementCounts.update(key, Counter(frequency, error))
    }

    objectStream.close()
    sketch
  }
}

case class Counter(frequency: Long, error: Long) extends Serializable
