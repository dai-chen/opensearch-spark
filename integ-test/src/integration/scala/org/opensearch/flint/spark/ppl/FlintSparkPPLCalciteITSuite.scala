/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.opensearch.sql.data.model.ExprValueUtils
import org.opensearch.sql.expression.datetime.DateTimeFunctions

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.types.{IntegerType, StringType}

class FlintSparkPPLCalciteITSuite
    extends QueryTest
    with LogicalPlanTestUtils
    with FlintPPLSuite
    with StreamTest {

  /** Test table and index name */
  private val testTable = "spark_catalog.default.flint_ppl_test"

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Create test table
    createPartitionedStateCountryTable(testTable)
  }

  test(s"test Calcite-PPL basic query") {
    val df = sql(s"source = $testTable | eval f = crc32(name) | fields f")
    df.explain(true)
    df.show
  }

  test(s"test Calcite-PPL basic query with UDF") {
    spark.udf.register(
      "get_format",
      (`type`: String, format: String) => {
        DateTimeFunctions
          .exprGetFormat(
            ExprValueUtils.fromObjectValue(`type`),
            ExprValueUtils.fromObjectValue(format))
          .valueForCalcite()
      },
      StringType)

    val df = sql(s"source = $testTable | eval f = GET_FORMAT(DATE, 'USA') | fields f")
    df.explain(true)
    df.show
  }

  test(s"test Calcite-PPL basic query with overridden UDF") {
    spark.udf.register(
      "char_length", // length is translated to char_length by SparkSQL dialect
      (s: String) => "abc",
      StringType)

    val query = s"source = $testTable | eval f = LENGTH(name) | fields f"
    val df = sql(query)
    df.explain(true)
    df.show
  }

  test(s"test Calcite-PPL basic query with nested built-in function") {
    val query =
      s"source = $testTable | eval f = SUBSTRING(SUBSTRING(name, 0 + 1), 1 + 1) | fields f"
    val df = sql(query)
    df.explain("codegen")
    df.show
  }

  test(s"test Calcite-PPL basic query with nested UDF") {
    spark.udf.register("substring", (s: String, pos: Int) => s.substring(pos), StringType)

    val query =
      s"source = $testTable | eval f = SUBSTRING(SUBSTRING(name, 0 + 1), 1 + 1) | fields f"
    val df = sql(query)
    df.explain("codegen")
    df.show
  }

  test(s"test Calcite-PPL basic query with all UDF registered") {

    val query = s"source = $testTable | eval f = LENGTH(name) | fields f"
    val df = sql(query)
    df.explain(true)
    df.show
  }
}
