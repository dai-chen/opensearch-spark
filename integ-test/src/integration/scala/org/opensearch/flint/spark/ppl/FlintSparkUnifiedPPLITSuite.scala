/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

/**
 * Integration tests for PPL query execution with unified parser.
 */
class FlintSparkUnifiedPPLITSuite extends FlintPPLSuite {

  private val testTable = "spark_catalog.default.flint_ppl_unified_test"

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(FlintPPLConf.PPL_UNIFIED_ENABLED_KEY, "true")
    createPeopleTable(testTable)
  }

  test("PPL query should execute with unified parser when enabled") {
    val result = sql(s"source=$testTable | fields id, name")
    assert(result.collect().length == 6)
  }
}
