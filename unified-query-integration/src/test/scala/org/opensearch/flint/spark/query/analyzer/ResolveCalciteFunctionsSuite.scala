/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query.analyzer

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression

class ResolveCalciteFunctionsSuite extends SparkFunSuite {

  test("ResolveCalciteFunctions resolves JSON_DELETE via Calcite") {
    val spark = SparkSession
      .builder()
      .appName("ResolveCalciteFunctionsSuite")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .withExtensions { extensions =>
        CalciteFunctionRegistration.descriptions.foreach(extensions.injectFunction)
        extensions.injectResolutionRule(session => ResolveCalciteFunctions(session))
      }
      .getOrCreate()

    try {
      import spark.implicits._

      Seq(("alice", """{"age":25,"city":"NYC"}"""), ("bob", """{"age":30,"city":"LA"}"""))
        .toDF("name", "data")
        .createOrReplaceTempView("json_delete_test")

      // Parse and analyze the query to verify JSON_DELETE is resolved
      val df = spark.sql("""
          |SELECT name,
          |       JSON_DELETE(data, array('age')) AS cleaned_data
          |FROM json_delete_test
          |WHERE name = 'alice'
          |""".stripMargin)

      // Verify the query was successfully analyzed (function resolved)
      val analyzedPlan = df.queryExecution.analyzed
      assert(analyzedPlan != null)

      // Verify that CalciteRexExpression is present in the plan
      import org.opensearch.flint.spark.query.expression.CalciteRexExpression

      // Recursively search for CalciteRexExpression in all expressions
      // (needed because CalciteRexExpression may be wrapped in Alias or other expressions)
      def containsCalciteExpression(expr: Expression): Boolean = {
        expr match {
          case _: CalciteRexExpression => true
          case _ => expr.children.exists(containsCalciteExpression)
        }
      }

      var hasCalciteExpression = false
      analyzedPlan.foreach { plan =>
        plan.expressions.foreach { expr =>
          if (containsCalciteExpression(expr)) {
            hasCalciteExpression = true
          }
        }
      }
      assert(
        hasCalciteExpression,
        "Expected CalciteRexExpression to be present in the analyzed plan")

      logInfo("Successfully resolved JSON_DELETE to CalciteRexExpression")

      // TODO: Execution test disabled - Calcite PPL function evaluation not yet implemented
      // The evaluation requires a specialized runtime environment for OpenSearch PPL functions
      // For now, we verify that:
      // 1. The function is recognized (no AnalysisException)
      // 2. It's resolved to CalciteRexExpression
      // 3. The query plan is valid
    } finally {
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }
}
