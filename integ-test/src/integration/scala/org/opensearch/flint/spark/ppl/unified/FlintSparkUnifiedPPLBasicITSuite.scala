/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl.unified

import org.opensearch.flint.spark.ppl.FlintSparkPPLBasicITSuite

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Runs all tests from FlintSparkPPLBasicITSuite using the unified (Calcite-based) PPL parser.
 */
class FlintSparkUnifiedPPLBasicITSuite
    extends FlintSparkPPLBasicITSuite
    with UnifiedPPLTestSupport {

  override def compareByString(plan: LogicalPlan): String = ""

  override protected def unsupportedTests: Set[String] = Set(
    "explain simple mode test",
    "explain extended mode test",
    "explain codegen mode test",
    "explain cost mode test",
    "explain formatted mode test",
    "describe (extended) table query test",
    "describe (extended) FQN (2 parts) table query test",
    "test explain backtick table names and name contains '.'")
}
