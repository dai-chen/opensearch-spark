/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.function.topksketch

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import scala.collection.mutable

/**
 * Space-Saving Sketch that implements TopKSketch. Tracks the Top K elements with the highest
 * estimated counts.
 */
class SpaceSavingBinaryTreeSketch(k: Int, tracked: Int)
    extends TopKSketch[String]
    with Serializable {

  override val name: String = "space_saving_binary_tree"

  // Map to store elements and their counts
  private val elementCounts = mutable.Map.empty[String, Long]

  // TreeMap to maintain counts and their corresponding items
  // Using TreeMap gives us O(log n) for finding min and updating
  private val countToItems = mutable.TreeMap.empty[Long, mutable.Set[String]]

  override def update(item: String): Unit = {
    update(item, 1)
  }

  override def update(item: String, increment: Long): Unit = {
    require(increment > 0, "increment must be positive")

    if (elementCounts.contains(item)) {
      // Remove from old count group
      val oldCount = elementCounts(item)
      removeFromCountGroup(item, oldCount)

      // Add to new count group
      val newCount = oldCount + increment
      elementCounts(item) = newCount
      addToCountGroup(item, newCount)

    } else if (elementCounts.size < tracked) {
      // Add new item if there's space
      elementCounts(item) = increment
      addToCountGroup(item, increment)

    } else {
      // Replace only if new weight would be greater than minimum
      val minCount = countToItems.firstKey
      val minItems = countToItems(minCount)
      val minItem = minItems.head

      // Remove the minimum item
      elementCounts.remove(minItem)
      removeFromCountGroup(minItem, minCount)

      // Add the new item
      val newCount = minCount + increment
      elementCounts(item) = newCount
      addToCountGroup(item, newCount)
    }
  }

  private def removeFromCountGroup(item: String, count: Long): Unit = {
    val items = countToItems.get(count)
    if (items.isDefined) {
      items.get.remove(item)
      if (items.get.isEmpty) {
        countToItems.remove(count)
      }
    }
  }

  private def addToCountGroup(item: String, count: Long): Unit = {
    countToItems.getOrElseUpdate(count, mutable.Set.empty) += item
  }

  override def merge(other: TopKSketch[String]): Unit = {
    other match {
      case ssAdapter: SpaceSavingBinaryTreeSketch =>
        ssAdapter.elementCounts.foreach { case (item, count) =>
          update(item, count)
        }

      case _ => throw new IllegalArgumentException("Cannot merge with incompatible sketch")
    }
  }

  override def getTopK: Seq[(String, Long)] = {
    // Use TreeMap's reverse ordering to get top K efficiently
    countToItems.toSeq.reverse // Reverse to get highest counts first
      .flatMap { case (count, items) =>
        items.map(item => (item, count))
      }
      .take(k)
  }

  override def serialize(): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    try {
      oos.writeObject(this)
      bos.toByteArray
    } finally {
      oos.close()
      bos.close()
    }
  }

  override def deserialize(bytes: Array[Byte]): TopKSketch[String] = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)
    try {
      ois.readObject().asInstanceOf[SpaceSavingBinaryTreeSketch]
    } finally {
      ois.close()
      bis.close()
    }
  }

  override def toString: String = {
    s"""
       | k = $k, tracked = $tracked
       | elementCounts = $elementCounts
       | countToItems = $countToItems
       |""".stripMargin
  }
}
