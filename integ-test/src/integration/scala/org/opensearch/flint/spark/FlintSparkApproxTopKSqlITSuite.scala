/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

class FlintSparkApproxTopKSqlITSuite extends FlintSparkSuite {

  private val testTable = s"$catalogName.default.approx_top_count_test"

  override def beforeEach(): Unit = {
    super.beforeAll()
    createTimeSeriesTransactionTable(testTable)
  }

  override def afterEach(): Unit = {
    super.afterEach()
    sql(s"DROP TABLE $testTable")
  }

  test("approx top count") {
    sql(s"""
        | SELECT approx_top_count(customerId, 2)
        | FROM $testTable
        |""".stripMargin).show(false)
  }
}
