/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.bloomfilter

import java.io.{DataInputStream, DataOutputStream, InputStream, OutputStream}

import org.opensearch.flint.spark.skipping.bloomfilter.AdaptiveBloomFilter.{K, RANGES}

import org.apache.spark.internal.Logging
import org.apache.spark.util.sketch.BloomFilter

class AdaptiveBloomFilter(val candidates: Array[BloomFilter]) extends BloomFilter with Logging {

  private var total: Long = 0

  override def cardinality(): Long = total

  // Each method will call each candidate in the array above
  // and meanwhile increment a counter total
  override def expectedFpp(): Double = throw new UnsupportedOperationException

  override def bitSize(): Long = candidates.map(_.bitSize()).sum

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

    val otherBf = other.asInstanceOf[AdaptiveBloomFilter]
    for ((bf1, bf2) <- candidates zip otherBf.candidates) {
      bf1.mergeInPlace(bf2)
    }
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
    candidates.foreach(_.writeTo(out))

    new DataOutputStream(out).writeInt(total.toInt)
  }

  def readFrom(in: InputStream): Unit = {
    total = new DataInputStream(in).readInt()
  }

  def bestCandidate(): BloomFilter = {
    val index = RANGES.indexWhere(range => total < range * K)
    candidates(index)
  }
}

object AdaptiveBloomFilter {

  val K: Int = 1024

  /** TODO: generate from input param such as expectedNDV? */
  val RANGES = Array(1, 2, 4, 8, 16, 32, 64, 128, 256, 512)

  def create(expectedNDV: Long): BloomFilter = {
    // Create an BF array of size 10
    // Each element is created by BloomFilter.create(expectedNDV)
    // in which expectedNDV is 1k, 2k, 4k, ... 512k
    val candidates = RANGES.map(_ * K).map(ndv => BloomFilter.create(ndv, 0.01))
    new AdaptiveBloomFilter(candidates)
  }

  def readFrom(in: InputStream): BloomFilter = {
    val candidates = Array.fill(10)(BloomFilter.readFrom(in))
    val bloomFilter = new AdaptiveBloomFilter(candidates)
    bloomFilter.readFrom(in)
    bloomFilter
  }
}
