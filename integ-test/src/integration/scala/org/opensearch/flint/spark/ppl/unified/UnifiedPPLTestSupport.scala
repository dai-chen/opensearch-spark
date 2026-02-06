/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl.unified

import org.opensearch.flint.spark.FlintUnifiedPPLSparkExtensions
import org.opensearch.flint.spark.ppl.{FlintPPLSuite, LogicalPlanTestUtils}
import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.flint.config.FlintSparkConf.OPTIMIZER_RULE_ENABLED

/**
 * Mixin trait that enables running PPL integration tests with the unified (Calcite-based) parser.
 * Mix this trait into existing PPL IT suites to run them using the unified parser instead of the
 * legacy parser.
 *
 * Example usage: {{{ class FlintSparkUnifiedPPLBasicITSuite extends FlintSparkPPLBasicITSuite
 * with UnifiedPPLTestSupport }}}
 *
 * Tests that fail here but pass in the original suite indicate unified parser issues. Tests that
 * pass here but fail in the original suite indicate legacy parser issues.
 *
 * This trait overrides plan comparison methods to focus on query result correctness rather than
 * exact logical plan structure, since the unified parser may produce semantically equivalent but
 * structurally different plans.
 */
trait UnifiedPPLTestSupport extends FlintPPLSuite {
  self: LogicalPlanTestUtils =>

  /**
   * Set of test names that are not supported by the unified parser. Subclasses can override this
   * to specify tests that should be ignored.
   */
  protected def unsupportedTests: Set[String] = Set.empty

  override protected def sparkConf: SparkConf = {
    // Get base config from parent, then override extensions
    val conf = super.sparkConf
    conf.set("spark.sql.extensions", classOf[FlintUnifiedPPLSparkExtensions].getName)
    conf.set(OPTIMIZER_RULE_ENABLED.key, "false")
    conf
  }

  /**
   * Override test registration to convert unsupported tests to ignored tests.
   */
  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    if (unsupportedTests.contains(testName)) {
      super.ignore(testName, testTags: _*)(testFun)(pos)
    } else {
      super.test(testName, testTags: _*)(testFun)(pos)
    }
  }

  /**
   * Override to bypass plan string comparison. Returns empty string so that assertions like
   * `assert(compareByString(expected) === compareByString(actual))` always pass.
   */
  override def compareByString(plan: LogicalPlan): String = ""

  /**
   * Override the comparePlans method from PlanTest to make it a no-op. This allows tests to focus
   * on query result correctness rather than exact plan structure.
   */
  override def comparePlans(
      plan1: LogicalPlan,
      plan2: LogicalPlan,
      checkAnalysis: Boolean = true): Unit = {
    // No-op: skip plan comparison for unified parser tests
  }
}
