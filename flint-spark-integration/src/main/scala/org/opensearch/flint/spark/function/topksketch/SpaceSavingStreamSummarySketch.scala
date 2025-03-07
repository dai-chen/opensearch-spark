/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.function.topksketch

import java.io._

import scala.collection.JavaConverters._

import org.opensearch.flint.spark.function.topksketch.streamsummary.StreamSummary

/**
 * A fast implementation of Space-Saving Top-K using StreamSummary.
 *
 * @param k
 *   The number of top elements to track.
 * @param tracked
 *   The internal capacity of StreamSummary.
 */
class SpaceSavingStreamSummarySketch(k: Int, tracked: Int)
    extends TopKSketch[String]
    with Serializable {

  override val name: String = "space_saving_stream_summary"

  // Validate input parameters
  require(k > 0, "k must be greater than 0")
  require(tracked >= k, "tracked must be at least k")

  // Calculate epsilon based on tracked (i.e., number of counters)
  private val epsilon: Double = 1.0 / tracked

  // Initialize StreamSummary with computed epsilon
  private val streamSummary = new StreamSummary[String](epsilon)

  /** Update the sketch with a key */
  override def update(item: String): Unit = {
    streamSummary.offer(item)
  }

  /** Update the sketch with a key and weight */
  def update(item: String, increment: Long): Unit = {
    streamSummary.offer(item, increment)
  }

  /** Merge another sketch into this one */
  override def merge(other: TopKSketch[String]): Unit = {
    other match {
      case otherSketch: SpaceSavingStreamSummarySketch =>
        otherSketch.streamSummary.getAll.forEach { counter =>
          update(counter.getItem, counter.getValue)
        }

      case _ => throw new IllegalArgumentException("Cannot merge with incompatible sketch")
    }
  }

  /** Retrieve the Top-K elements */
  override def getTopK: Seq[(String, Long)] = {
    streamSummary.getTopK(k).asScala.map(counter => (counter.getItem, counter.getValue))
  }

  /** Serialize the sketch */
  override def serialize(): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(this)
    oos.close()
    bos.toByteArray
  }

  /** Deserialize the sketch */
  override def deserialize(bytes: Array[Byte]): SpaceSavingStreamSummarySketch = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)
    val obj = ois.readObject().asInstanceOf[SpaceSavingStreamSummarySketch]
    ois.close()
    obj
  }
}
