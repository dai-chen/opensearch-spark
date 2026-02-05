/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl.unified

import org.opensearch.flint.spark.ppl.FlintSparkPPLBasicITSuite

/**
 * Runs all tests from FlintSparkPPLBasicITSuite using the unified (Calcite-based) PPL parser.
 */
class FlintSparkUnifiedPPLBasicITSuite
    extends FlintSparkPPLBasicITSuite
    with UnifiedPPLTestSupport {

  override protected def unsupportedTests: Set[String] = Set(
    // Unified PPL support explain in different syntax
    "explain simple mode test",
    "explain extended mode test",
    "explain codegen mode test",
    "explain cost mode test",
    "explain formatted mode test",
    // Unified PPL doesn't support describe command
    "describe (extended) table query test",
    "describe (extended) FQN (2 parts) table query test",
    // FIXME: quote table name before spark.table() call
    "test backtick table names and name contains '.'",
    "test describe backtick table names and name contains '.'",
    "test explain backtick table names and name contains '.'",
    "test table name with more than 3 parts",
    // Unified PPL doesn't support search multiple tables
    "Search multiple tables - translated into union call with fields",
    "Search multiple tables - with table alias")
}
