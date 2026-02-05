/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.opensearch.flint.spark.FlintPPLSuite
import org.scalatest.matchers.should.Matchers

class FlintPPLConfSuite extends FlintPPLSuite with Matchers {

  test("test PPL unified enabled conf") {
    // default value
    spark.conf.unset(FlintPPLConf.PPL_UNIFIED_ENABLED_KEY)
    FlintPPLConf(spark).isPPLUnifiedEnabled shouldBe false

    // explicit values
    spark.conf.set(FlintPPLConf.PPL_UNIFIED_ENABLED_KEY, "true")
    FlintPPLConf(spark).isPPLUnifiedEnabled shouldBe true

    spark.conf.set(FlintPPLConf.PPL_UNIFIED_ENABLED_KEY, "false")
    FlintPPLConf(spark).isPPLUnifiedEnabled shouldBe false

    spark.conf.unset(FlintPPLConf.PPL_UNIFIED_ENABLED_KEY)
  }
}
