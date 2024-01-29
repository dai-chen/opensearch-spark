/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.bloomfilter

import java.io.{DataInputStream, DataOutputStream, InputStream, OutputStream}

import org.apache.spark.internal.Logging
import org.apache.spark.util.sketch.BloomFilter

case class AdaptiveBloomFilter2(bloomFilter: BloomFilter) extends BloomFilter with Logging {

  private var total: Int = 0

  override def cardinality(): Long = total

  // Each method will call each candidate in the array above
  // and meanwhile increment a counter total
  override def expectedFpp(): Double = throw new UnsupportedOperationException

  override def bitSize(): Long = bloomFilter.bitSize()

  override def put(item: Any): Boolean = throw new UnsupportedOperationException

  override def putString(item: String): Boolean = throw new UnsupportedOperationException

  override def putLong(item: Long): Boolean = {
    // Use the last BF put result (which is most accurate)
    val bitChanged = bloomFilter.putLong(item)

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
    val otherBf = other.asInstanceOf[AdaptiveBloomFilter2]
    total = total + otherBf.total
    bloomFilter.mergeInPlace(otherBf.bloomFilter)
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
    bloomFilter.writeTo(out)

    val dos = new DataOutputStream(out)
    dos.writeInt(total)
  }

  def readFrom(in: InputStream): BloomFilter = {
    val dis = new DataInputStream(in)
    this.total = dis.readInt()
    this
  }
}

object AdaptiveBloomFilter2 {

  def create(expectedNDV: Long): BloomFilter = {
    new AdaptiveBloomFilter2(BloomFilter.create(expectedNDV))
  }

  def readFrom(in: InputStream): BloomFilter = {
    val bloomFilter = BloomFilter.readFrom(in)
    new AdaptiveBloomFilter2(bloomFilter).readFrom(in)
  }
}
