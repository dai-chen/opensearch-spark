/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query

import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.{reset, verify, when}
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.test.SharedSparkSession

class UnifiedQuerySparkParserSuite extends SharedSparkSession {
  private var unifiedParser: UnifiedQuerySparkParser = _
  private var mockSparkParser: ParserInterface = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    mockSparkParser = mock[ParserInterface]
    unifiedParser = new UnifiedQuerySparkParser(spark, mockSparkParser)

    sql("CREATE TABLE foo (id INT, name STRING) USING JSON")
    sql("CREATE DATABASE db2")
    sql("CREATE TABLE db2.bar (id INT, value STRING) USING JSON")
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    reset(mockSparkParser)
    when(mockSparkParser.parsePlan(anyString())).thenReturn(mock[LogicalPlan])
  }

  test("should translate PPL to SQL and delegate to Spark parser") {
    unifiedParser.parsePlan("source = spark_catalog.default.foo | fields name")
    verify(mockSparkParser)
      .parsePlan("SELECT `name`\nFROM `spark_catalog`.`default`.`foo`")
  }

  test("should translate PPL with unqualified table name and delegate to Spark parser") {
    unifiedParser.parsePlan("source = foo | fields name")
    verify(mockSparkParser)
      .parsePlan("SELECT `name`\nFROM `spark_catalog`.`default`.`foo`")
  }

  // TODO: need to figure out later since this is valid in Spark
  test("should throw exception for table name with database only") {
    assertThrows[IllegalStateException] {
      unifiedParser.parsePlan("source = default.foo | fields name")
    }
  }

  test("should translate PPL across databases and delegate to Spark parser") {
    unifiedParser.parsePlan(
      "source = spark_catalog.default.foo | lookup spark_catalog.db2.bar id | fields name, value")
    verify(mockSparkParser).parsePlan("""SELECT `foo`.`name`, `bar`.`value`
        |FROM `spark_catalog`.`default`.`foo`
        |LEFT JOIN `spark_catalog`.`db2`.`bar` ON `foo`.`id` = `bar`.`id`""".stripMargin)
  }

  Seq("source = spark_catalog.db2.bar | fields value", "source = bar | fields value").foreach {
    pplText =>
      test(s"should translate PPL using other database and delegate to Spark parser: $pplText") {
        sql("USE db2")
        unifiedParser.parsePlan(pplText)
        verify(mockSparkParser).parsePlan("SELECT `value`\nFROM `spark_catalog`.`db2`.`bar`")
      }
  }

  test("should fall back to Spark parser on unsupported query") {
    val invalid = "not a PPL query"
    unifiedParser.parsePlan(invalid)
    verify(mockSparkParser).parsePlan(invalid)
  }
}
