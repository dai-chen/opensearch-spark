/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.bloomfilter

import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.util.sketch.BloomFilter

class AdaptiveBloomFilterSuite extends SparkFunSuite with Matchers {

  // Insert 50K
  // 50K -> total = 49687
  // 100K -> total = 49983
  // 250K -> total = 50000
  // 500K -> total = 50000
  test("static bf") {
    val bf = BloomFilter.create(250000)

    // scalastyle:off println
    // println(bf.bitSize())

    var total = 0
    1 to 50000 foreach { i => if (bf.putLong(i)) total = total + 1 }

    println(total)
    // scalastyle:on println
  }

  // BF NDV:512. Insert 500. Cardinality=498
  // BF NDV:512K. Insert 500K. Cardinality=497353
  test("adaptive bf") {
    val bf = AdaptiveBloomFilter.create(100).asInstanceOf[AdaptiveBloomFilter]

    // scalastyle:off println
    1 to 50000 foreach { i => bf.putLong(i) }
    println(bf.cardinality())

    // Doesn't matter because BF won't give false positive
    // For existing element, BF always give correct answer
    // 1 to 100000 foreach { i => bf.putLong(i) }
    // println(bf.cardinality())

    println(bf.bestCandidate().bitSize())
    // scalastyle:on println
  }
}
