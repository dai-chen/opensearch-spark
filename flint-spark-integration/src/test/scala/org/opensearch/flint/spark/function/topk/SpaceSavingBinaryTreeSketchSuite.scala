/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.function.topk

import org.opensearch.flint.spark.function.topksketch.SpaceSavingBinaryTreeSketch
import org.scalatest.matchers.should.Matchers

import org.apache.spark.FlintSuite

class SpaceSavingBinaryTreeSketchSuite extends FlintSuite with Matchers {

  test("space saving update") {
    val sketch = new SpaceSavingBinaryTreeSketch(2, 5)

    sketch.update("apple")
    sketch.update("apple")
    sketch.update("apple")
    logInfo(s"TopK: ${sketch.getTopK}")
    logInfo(s"Internal: $sketch")

    sketch.update("orange")
    sketch.update("orange")
    sketch.update("banana")
    sketch.update("watermelon")
    sketch.update("grape")
    logInfo(s"TopK: ${sketch.getTopK}")
    logInfo(s"Internal: $sketch")

    // Full and swap
    sketch.update("pineapple")
    logInfo(s"TopK: ${sketch.getTopK}")
    logInfo(s"Internal: $sketch")

    sketch.update("mango")
    logInfo(s"TopK: ${sketch.getTopK}")
    logInfo(s"Internal: $sketch")

    sketch.update("pineapple")
    logInfo(s"TopK: ${sketch.getTopK}")
    logInfo(s"Internal: $sketch")
  }

  test("space saving merge") {
    val sketch1 = new SpaceSavingBinaryTreeSketch(2, 5)
    sketch1.update("apple")
    sketch1.update("apple")
    sketch1.update("apple")
    sketch1.update("orange")
    sketch1.update("orange")
    sketch1.update("banana")

    val sketch2 = new SpaceSavingBinaryTreeSketch(2, 5)
    sketch1.update("apple")
    sketch1.update("apple")
    sketch2.update("grape")
    sketch2.update("pineapple")
    sketch2.update("watermelon")

    sketch1.merge(sketch2)
    logInfo(s"Sketch: $sketch1")
  }

  test("space saving serialize and deserialize") {
    val sketch1 = new SpaceSavingBinaryTreeSketch(2, 5)
    sketch1.update("apple")
    sketch1.update("apple")
    sketch1.update("apple")
    sketch1.update("orange")
    sketch1.update("orange")
    sketch1.update("banana")

    val sketch2 = sketch1.deserialize(sketch1.serialize())
    logInfo(s"Sketch: $sketch2")
  }
}
