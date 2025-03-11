/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.function.topksketch

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import scala.collection.mutable

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

  override val name: String = "space_saving_parallel"

  case class Counter(var count: Long, var error: Long, var slot: Int = 0, var hash: Int = 0)

  private val counterMap = mutable.HashMap.empty[String, Counter]
  private val alphaMap = new Array[Long](nextAlphaSize(tracked))
  private val counterList = mutable.ArrayBuffer.empty[(String, Counter)]
  private var removedKeys: Int = 0

  private def nextAlphaSize(x: Long): Int = {
    val alphaMapElementsPerCounter = 6
    (1L << (64 - java.lang.Long.numberOfLeadingZeros(x * alphaMapElementsPerCounter))).toInt
  }

  private def push(item: String, counter: Counter): Unit = {
    counter.slot = counterList.size
    counterList.append((item, counter))
    counterMap(item) = counter
    percolate(counter)
  }

  // This is equivalent to one step of bubble sort
  private def percolate(counter: Counter): Unit = {
    while (counter.slot > 0) {
      val prevIdx = counter.slot - 1
      val prev = counterList(prevIdx)._2

      if (counter.count > prev.count ||
        (counter.count == prev.count && counter.error < prev.error)) {
        // Swap elements
        val temp = counterList(counter.slot)
        counterList(counter.slot) = counterList(prevIdx)
        counterList(prevIdx) = temp

        // Update slots
        val tempSlot = counter.slot
        counter.slot = prev.slot
        prev.slot = tempSlot
      } else {
        return
      }
    }
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
        percolate(counter)
        return
      case None =>
    }

    // Case 2: Space available in tracking list
    if (counterList.size < tracked) {
      val counter = Counter(increment, error, counterList.size, hash)
      push(item, counter)
      return
    }

    val minCounter = counterList.last._2

    // Case 3: New key has bigger weight than minimum counter
    // This case is important for weighted top-k
    if (increment > minCounter.count) {
      destroyLastElement()
      val counter = Counter(increment, error, counterList.size, hash)
      push(item, counter)
      return
    }

    // Case 4: Need to use alpha map and possibly replace minimum
    val alphaMask = alphaMap.length - 1
    val alphaIdx = (hash & alphaMask).toInt

    if (alphaMap(alphaIdx) + increment < minCounter.count) {
      alphaMap(alphaIdx) += increment
    } else {
      alphaMap(alphaIdx) = minCounter.count
      destroyLastElement()

      val newCounter = Counter(
        count = alphaMap(alphaIdx) + increment,
        error = alphaMap(alphaIdx) + error,
        slot = counterList.size,
        hash = hash)
      push(item, newCounter)
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
        for (other <- ss.counterList.reverseIterator) { // Note: scanning in reverse as per ClickHouse
          val (item, otherCounter) = other
          counterMap.get(item) match {
            case Some(counter) =>
              // Subtract m2 previously added, guaranteed not negative
              counter.count += (otherCounter.count - m2)
              counter.error += (otherCounter.error - m2)
            case None =>
              // Counters not monitored in S1
              val newCounter = Counter(
                count = otherCounter.count + m1,
                error = otherCounter.error + m1,
                slot = counterList.size,
                hash = item.hashCode)
              counterMap(item) = newCounter
              counterList.append((item, newCounter))
          }
        }

        // Sort and trim to capacity
        import scala.collection.JavaConverters._
        java.util.Collections.sort(
          counterList.asJava,
          (a: (String, Counter), b: (String, Counter)) => {
            if (a._2.count > b._2.count ||
              (a._2.count == b._2.count && a._2.error < b._2.error)) { -1 }
            else if (a._2.count == b._2.count && a._2.error == b._2.error) { 0 }
            else { 1 }
          })

        if (counterList.size > tracked) {
          counterList.remove(tracked, counterList.size - tracked)
        }

        // Update slots after sorting
        for (i <- counterList.indices) {
          counterList(i)._2.slot = i
        }
        rebuildCounterMap()
    }
  }

  private def destroyLastElement(): Unit = {
    val (item, _) = counterList.last
    counterMap.remove(item)
    counterList.remove(counterList.size - 1)

    removedKeys += 1
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

  override def getTopK: Seq[(String, Long, Long)] = {
    counterList.take(k).map { case (item, counter) => (item, counter.count, counter.error) }
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

  override def toString: String =
    s"""
      | k = $k, tracked = $tracked
      | counterMap = $counterMap
      | alphaMap = ${alphaMap.mkString("Array(", ", ", ")")}
      | counterList = $counterList
      | removedKeys = $removedKeys
      |""".stripMargin
}
