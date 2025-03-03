/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.function.topksketch

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.ByteBuffer
import java.util.Base64
import scala.collection.mutable

/**
 * Efficient Parallel Space-Saving Sketch implementation for Top-K estimation.
 * Implements the reduce-and-combine strategy from:
 * - "Parallel Space-Saving: https://arxiv.org/pdf/1401.0702.pdf"
 * - "Finding top-k elements in data streams: http://www.l2f.inesc-id.pt/~fmmb/wiki/uploads/Work/misnis.ref0a.pdf"
 */
class ParallelSpaceSavingSketch(k: Int, tracked: Int) extends TopKSketch[String] with Serializable {
    case class Counter(
                        var count: Long,
                        var error: Long = 0
                      ) extends Serializable

    // Main counter map to store elements and their counts/errors
    private val counterMap = mutable.HashMap.empty[String, Counter]

    // Alpha map for monitoring frequency of untracked elements
    private val alphaMap = new Array[Long](nextAlphaSize(tracked))

    // Sorted list of elements by count (descending)
    private val counterList = mutable.ArrayBuffer.empty[(String, Counter)]

    private def nextAlphaSize(x: Int): Int = {
      val alphaMapElementsPerCounter = 6
      1 << (64 - java.lang.Long.numberOfLeadingZeros(x * alphaMapElementsPerCounter - 1))
    }

    override def update(item: String): Unit = {
      val hash = item.hashCode

      // Case 1: Item already exists in counter map
      counterMap.get(item) match {
        case Some(counter) =>
          counter.count += 1
          reorderCounters()
          return
        case None => // Continue to other cases
      }

      // Case 2: Space available in tracking list
      if (counterList.size < tracked) {
        val counter = Counter(1)
        counterMap(item) = counter
        counterList.append((item, counter))
        reorderCounters()
        return
      }

      // Case 3: Need to use alpha map and possibly replace minimum
      val alphaMask = alphaMap.length - 1
      val alphaIdx = (hash & alphaMask).toInt
      val minCounter = counterList.last._2

      if (alphaMap(alphaIdx) + 1 < minCounter.count) {
        alphaMap(alphaIdx) += 1
      } else {
        // Replace minimum element
        val (minItem, _) = counterList.last
        counterMap.remove(minItem)

        val newCounter = Counter(
          count = alphaMap(alphaIdx) + 1,
          error = alphaMap(alphaIdx)
        )

        alphaMap(alphaIdx) = minCounter.count
        counterMap(item) = newCounter
        counterList(counterList.size - 1) = (item, newCounter)
        reorderCounters()
      }
    }

    override def merge(other: TopKSketch[String]): Unit = {
      other match {
        case ss: ParallelSpaceSavingSketch =>
          // Calculate m1 and m2 (minimum counts from both sketches)
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
                val newCounter = Counter(
                  count = otherCounter.count + m1,
                  error = otherCounter.error + m1
                )
                counterMap(item) = newCounter
                counterList.append((item, newCounter))
            }
          }

          // Maintain only top tracked elements
          reorderCounters()
          if (counterList.size > tracked) {
            val toRemove = counterList.drop(tracked)
            toRemove.foreach { case (item, _) => counterMap.remove(item) }
            counterList.remove(tracked, counterList.size - tracked)
          }

        case _ => throw new IllegalArgumentException("Cannot merge with incompatible sketch")
      }
    }

  private def reorderCounters(): Unit = {
    // In-place sorting using ArrayBuffer's sortWith
    import scala.collection.JavaConverters._
    java.util.Collections.sort(counterList.asJava, (a: (String, Counter), b: (String, Counter)) => {
      val countCompare = java.lang.Long.compare(b._2.count, a._2.count) // descending
      if (countCompare != 0) countCompare
      else java.lang.Long.compare(a._2.error, b._2.error) // ascending
    })
  }

  override def getTopK: Seq[(String, Long)] = {
    counterList.take(k).map { case (item, counter) => (item, counter.count) }
  }


  override def serialize(): Array[Byte] = {
    val byteStream = new ByteArrayOutputStream()
    val objectStream = new ObjectOutputStream(byteStream)
    try {
      // Write the state
      objectStream.writeObject(counterMap)
      objectStream.writeObject(counterList)
      objectStream.writeObject(alphaMap)
      byteStream.toByteArray
    } finally {
      objectStream.close()
      byteStream.close()
    }
  }

  override def deserialize(bytes: Array[Byte]): TopKSketch[String] = {
    val byteStream = new ByteArrayInputStream(bytes)
    val objectStream = new ObjectInputStream(byteStream)
    try {
      // Read the state in the same order as serialized
      counterMap.clear()
      counterMap ++= objectStream.readObject().asInstanceOf[mutable.HashMap[String, Counter]]

      counterList.clear()
      counterList ++= objectStream.readObject().asInstanceOf[mutable.ArrayBuffer[(String, Counter)]]

      val newAlphaMap = objectStream.readObject().asInstanceOf[Array[Long]]
      Array.copy(newAlphaMap, 0, alphaMap, 0, alphaMap.length)

      this
    } finally {
      objectStream.close()
      byteStream.close()
    }
  }
  }
