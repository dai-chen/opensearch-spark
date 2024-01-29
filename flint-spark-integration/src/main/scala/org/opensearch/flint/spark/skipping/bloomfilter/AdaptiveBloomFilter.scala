/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.bloomfilter

import java.io.OutputStream

import org.opensearch.flint.spark.skipping.bloomfilter.AdaptiveBloomFilter.K

import org.apache.spark.internal.Logging
import org.apache.spark.util.sketch.BloomFilter

class AdaptiveBloomFilter(val expectedNDV: Long) extends BloomFilter with Logging {

  private var total: Long = 0

  private val ranges = Array(1, 2, 4, 8, 16, 32, 64, 128, 256, 512)

  // Create an BF array of size 10
  // Each element is created by BloomFilter.create(expectedNDV)
  // in which expectedNDV is 1k, 2k, 4k, ... 512k
  private val candidates: Array[BloomFilter] =
    (1 to 512).map(_ * K).map(ndv => BloomFilter.create(ndv)).toArray

  override def cardinality(): Long = total

  // Each method will call each candidate in the array above
  // and meanwhile increment a counter total
  override def expectedFpp(): Double = throw new UnsupportedOperationException

  override def bitSize(): Long = bestCandidate().bitSize()

  override def put(item: Any): Boolean = throw new UnsupportedOperationException

  override def putString(item: String): Boolean = throw new UnsupportedOperationException

  override def putLong(item: Long): Boolean = {
    // Use the last BF put result (which is most accurate)
    val bitChanged = candidates.map(_.putLong(item)).last

    // Bit changed means this is the first time for this item inserted to BF
    if (bitChanged) {
      total = total + 1
    }
    bitChanged
  }

  override def putBinary(item: Array[Byte]): Boolean = throw new UnsupportedOperationException

  // Ignore all these method since we only care about BF building
  override def isCompatible(other: BloomFilter): Boolean = throw new UnsupportedOperationException

  // This is called after each Partial Aggregate complete. So we just care about best candidate ???
  override def mergeInPlace(other: BloomFilter): BloomFilter = {
    total = total + other.cardinality()
    candidates.foreach(bf => bf.mergeInPlace(other))
    this
  }

  override def intersectInPlace(other: BloomFilter): BloomFilter =
    throw new UnsupportedOperationException

  override def mightContain(item: Any): Boolean = throw new UnsupportedOperationException

  override def mightContainString(item: String): Boolean = throw new UnsupportedOperationException

  override def mightContainLong(item: Long): Boolean = throw new UnsupportedOperationException

  override def mightContainBinary(item: Array[Byte]): Boolean =
    throw new UnsupportedOperationException

  // Check total and choose BF candidate close to but not exceed the total counter
  override def writeTo(out: OutputStream): Unit = {
    bestCandidate().writeTo(out)
  }

  private def bestCandidate(): BloomFilter = {
    val index = ranges.indexWhere(range => total < range * K)
    logWarning(
      s"NDV $total: choose candidate $index whose size is ${candidates(index).bitSize()}")

    candidates(index)
  }
}

object AdaptiveBloomFilter {

  val K: Int = 1024;

  def create(expectedNDV: Long): BloomFilter = {
    new AdaptiveBloomFilter(expectedNDV)
  }
}
