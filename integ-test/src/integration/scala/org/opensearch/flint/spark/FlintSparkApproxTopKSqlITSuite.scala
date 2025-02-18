/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.opensearch.action.search.{SearchRequest, SearchRequestBuilder}
import org.opensearch.client.RequestOptions
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView.getFlintIndexName
import org.opensearch.index.query.QueryBuilders

class FlintSparkApproxTopKSqlITSuite extends FlintSparkSuite {

  private val testTable = s"$catalogName.default.approx_top_count_test"
  private val mvName = s"$catalogName.default.approx_top_count_mv_test"
  private val flintIndexName = getFlintIndexName(mvName)

  override def beforeEach(): Unit = {
    super.beforeAll()
    createTimeSeriesTransactionTable(testTable)
  }

  override def afterEach(): Unit = {
    super.afterEach()
    deleteTestIndex(flintIndexName)
    sql(s"DROP TABLE $testTable")
  }

  Seq("accurate", "misra_gries", "cms", "space_saving").foreach { (algorithm) =>
    test(s"approx top count by $algorithm algorithm") {
      val approx_top_count = s"approx_top_count_$algorithm"
      sql(s"""
           | SELECT
           |   window.start,
           |   $approx_top_count(productId, 5, 10),
           |   $approx_top_count(customerId, 2, 10)
           | FROM $testTable
           | GROUP BY TUMBLE(transactionDate, '1 week')
           |""".stripMargin).show(false)
    }
  }

  test(s"approx top sum") {
    sql(s"""
           | SELECT
           |   window.start,
           |   approx_top_sum(productId, productsAmount, 5, 10),
           |   approx_top_sum(struct(productId, customerId), productsAmount, 5, 10)
           | FROM $testTable
           | GROUP BY TUMBLE(transactionDate, '1 week')
           |""".stripMargin).show(false)
  }

  Seq("accurate", "misra_gries", "cms", "space_saving").foreach { (algorithm) =>
    test(s"approx top count by $algorithm algorithm with auto-refresh MV") {
      withTempDir { checkpointDir =>
        val approx_top_count = s"approx_top_count_$algorithm"
        sql(s"""
             | CREATE MATERIALIZED VIEW $mvName
             | AS
             | SELECT
             |   window.start,
             |   $approx_top_count(productId, 5, 10),
             |   $approx_top_count(customerId, 2, 10)
             | FROM $testTable
             | GROUP BY TUMBLE(transactionDate, '1 week')
             | WITH (
             |   auto_refresh = true,
             |   checkpoint_location = '${checkpointDir.getAbsolutePath}',
             |   watermark_delay = '1 Minute'
             | )
             |""".stripMargin).show(false)

        val jobId = spark.streams.active.find(_.name == flintIndexName).get.id.toString
        awaitStreamingComplete(jobId)

        // Nested field bug
        // flint.queryIndex(flintIndexName).show(false)
        val request = new SearchRequest(flintIndexName)
        request.source().query(QueryBuilders.matchAllQuery())
        val response = openSearchClient.search(request, RequestOptions.DEFAULT)
        logInfo("Response: " + response)
      }
    }
  }
}
