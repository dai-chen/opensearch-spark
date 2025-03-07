/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.function.topksketch

trait TopKSketch[T] {

  val name: String

  /** Updates the sketch with a new item */
  def update(item: T): Unit

  def update(item: T, increment: Long): Unit

  /** Merges another sketch into this one */
  def merge(other: TopKSketch[T]): Unit

  /** Retrieves the Top K items and their estimated counts */
  def getTopK: Seq[(T, Long)]

  /** Serializes the sketch to a byte array */
  def serialize(): Array[Byte]

  /** Deserializes a sketch from a byte array */
  def deserialize(bytes: Array[Byte]): TopKSketch[T]
}
