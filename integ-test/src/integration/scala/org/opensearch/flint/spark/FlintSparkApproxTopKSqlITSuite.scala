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

  Seq("accurate", "misra_gries", "cms", "space_saving").foreach { (algorithm) =>
    test(s"approx top count by $algorithm algorithm") {
      val approx_top_count = s"approx_top_count_$algorithm"
      sql(s"""
           | SELECT
           |   window.start,
           |   $approx_top_count(productId, 5),
           |   $approx_top_count(customerId, 2)
           | FROM $testTable
           | GROUP BY TUMBLE(transactionDate, '1 week')
           |""".stripMargin).show(false)
    }
  }

  test(s"approx top sum") {
    sql(s"""
           | SELECT
           |   window.start,
           |   approx_top_sum(productId, productsAmount, 5)
           | FROM $testTable
           | GROUP BY TUMBLE(transactionDate, '1 week')
           |""".stripMargin).show(false)
  }
}
