/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.function.topksketch

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.ByteBuffer
import java.util.Base64

import scala.collection.mutable

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}

/**
 * Efficient Parallel Space-Saving Sketch implementation for Top-K estimation. Implements the
 * reduce-and-combine strategy from:
 *   - "Parallel Space-Saving: https://arxiv.org/pdf/1401.0702.pdf"
 *   - "Finding top-k elements in data streams:
 *     http://www.l2f.inesc-id.pt/~fmmb/wiki/uploads/Work/misnis.ref0a.pdf"
 */
class ParallelSpaceSavingSketch(k: Int, tracked: Int)
    extends TopKSketch[String]
    with Serializable {

  // Main counter map to store elements and their counts/errors
  private val counterMap = mutable.HashMap.empty[String, Counter]

  // Alpha map for monitoring frequency of untracked elements
  private val alphaMap = new Array[Long](nextAlphaSize(tracked))

  // Sorted list of elements by count (descending)
  private val counterList = mutable.ArrayBuffer.empty[(String, Counter)]

  // Also, let's track removed keys
  private var removedKeys: Int = 0

  private def nextAlphaSize(x: Int): Int = {
    val alphaMapElementsPerCounter = 6
    1 << (64 - java.lang.Long.numberOfLeadingZeros(x * alphaMapElementsPerCounter - 1))
  }

  override def update(item: String): Unit = {
    update(item, 1)
  }

  override def update(item: String, increment: Long): Unit = {
    update(item, increment, 0)
  }

  def update(item: String, increment: Long = 1, error: Long = 0): Unit = {
    val hash = item.hashCode

    // Case 1: Item already exists in counter map
    counterMap.get(item) match {
      case Some(counter) =>
        counter.count += increment
        counter.error += error
        reorderCounters()
        return
      case None =>
    }

    // Case 2: Space available in tracking list
    if (counterList.size < tracked) {
      val counter = Counter(increment, error)
      counterMap(item) = counter
      counterList.append((item, counter))
      return
    }

    // Case 3: Need to use alpha map and possibly replace minimum
    val alphaMask = alphaMap.length - 1
    val alphaIdx = (hash & alphaMask).toInt
    val minCounter = counterList.last._2

    if (alphaMap(alphaIdx) + increment < minCounter.count) {
      alphaMap(alphaIdx) += increment
    } else {
      // Use destroyLastElement instead of direct removal
      alphaMap(alphaIdx) = minCounter.count
      destroyLastElement()

      val newCounter =
        Counter(count = alphaMap(alphaIdx) + increment, error = alphaMap(alphaIdx) + error)

      counterMap(item) = newCounter
      counterList.append((item, newCounter))
    }
  }

  override def merge(other: TopKSketch[String]): Unit = {
    other match {
      case ss: ParallelSpaceSavingSketch =>
        val m1 = if (counterList.size == tracked) counterList.last._2.count else 0L
        val m2 = if (ss.counterList.size == tracked) ss.counterList.last._2.count else 0L

        // Add m2 to all existing counters
        if (m2 > 0) {
          counterList.foreach { case (_, counter) =>
            counter.count += m2
            counter.error += m2
          }
        }

        // Merge other sketch's counters
        ss.counterList.foreach { case (item, otherCounter) =>
          counterMap.get(item) match {
            case Some(counter) =>
              counter.count += (otherCounter.count - m2)
              counter.error += (otherCounter.error - m2)
            case None =>
              val newCounter =
                Counter(count = otherCounter.count + m1, error = otherCounter.error + m1)
              counterMap(item) = newCounter
              counterList.append((item, newCounter))
          }
        }

        // Sort and trim to capacity
        reorderCounters()
        if (counterList.size > tracked) {
          val toRemove = counterList.drop(tracked)
          toRemove.foreach { case (item, _) => counterMap.remove(item) }
          counterList.remove(tracked, counterList.size - tracked)
        }

        // Rebuild counter map to ensure consistency
        rebuildCounterMap()

      case _ => throw new IllegalArgumentException("Cannot merge with incompatible sketch")
    }
  }

  private def reorderCounters(): Unit = {
    // In-place sorting using ArrayBuffer's sortWith
    import scala.collection.JavaConverters._
    java.util.Collections.sort(
      counterList.asJava,
      (a: (String, Counter), b: (String, Counter)) => {
        val countCompare = java.lang.Long.compare(b._2.count, a._2.count) // descending
        if (countCompare != 0) countCompare
        else java.lang.Long.compare(a._2.error, b._2.error) // ascending
      })
  }

  override def getTopK: Seq[(String, Long)] = {
    counterList.take(k).map { case (item, counter) => (item, counter.count) }
  }

  override def serialize(): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(baos)

    try {
      // Write counterMap size and entries
      dos.writeInt(counterMap.size)
      for ((key, counter) <- counterMap) {
        dos.writeUTF(key)
        dos.writeLong(counter.count)
        dos.writeLong(counter.error)
      }

      // Write counterList size and entries
      dos.writeInt(counterList.size)
      for ((key, counter) <- counterList) {
        dos.writeUTF(key)
        dos.writeLong(counter.count)
        dos.writeLong(counter.error)
      }

      // Write alphaMap
      dos.writeInt(alphaMap.length)
      for (alpha <- alphaMap) {
        dos.writeLong(alpha)
      }

      dos.flush()
      baos.toByteArray
    } finally {
      dos.close()
      baos.close()
    }
  }

  override def deserialize(bytes: Array[Byte]): TopKSketch[String] = {
    val bis = new ByteArrayInputStream(bytes)
    val dis = new DataInputStream(bis)

    try {
      // Read counterMap
      counterMap.clear()
      val mapSize = dis.readInt()
      for (_ <- 0 until mapSize) {
        val key = dis.readUTF()
        val count = dis.readLong()
        val error = dis.readLong()
        counterMap(key) = Counter(count, error)
      }

      // Read counterList
      counterList.clear()
      val listSize = dis.readInt()
      for (_ <- 0 until listSize) {
        val key = dis.readUTF()
        val count = dis.readLong()
        val error = dis.readLong()
        counterList.append((key, Counter(count, error)))
      }

      // Read alphaMap
      val alphaSize = dis.readInt()
      for (i <- 0 until alphaSize) {
        alphaMap(i) = dis.readLong()
      }

      this
    } finally {
      dis.close()
      bis.close()
    }
  }

  private def destroyLastElement(): Unit = {
    val (item, _) = counterList.last
    counterMap.remove(item)
    counterList.remove(counterList.size - 1)

    removedKeys += 1
    // Rebuild counter map if too many removals have occurred
    if (removedKeys * 2 > counterMap.size) {
      rebuildCounterMap()
    }
  }

  private def rebuildCounterMap(): Unit = {
    removedKeys = 0
    counterMap.clear()
    counterList.foreach { case (item, counter) =>
      counterMap(item) = counter
    }
  }
}
