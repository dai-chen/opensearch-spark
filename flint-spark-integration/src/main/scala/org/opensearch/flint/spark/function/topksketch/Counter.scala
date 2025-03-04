/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.function.topksketch

@SerialVersionUID(1L) // Add this
class Counter extends Serializable {
  var count: Long = 0
  var error: Long = 0
}

object Counter {
  def apply(count: Long, error: Long = 0): Counter = {
    val c = new Counter()
    c.count = count
    c.error = error
    c
  }
}
