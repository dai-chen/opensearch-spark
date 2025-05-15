/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.calcite

import org.apache.calcite.sql.{SqlCall, SqlWriter}
import org.apache.calcite.sql.dialect.SparkSqlDialect
import org.apache.calcite.sql.fun.SqlStdOperatorTable

/**
 * Custom SqlDialect for Spark that properly handles table functions.
 */
class CustomSparkSqlDialect extends SparkSqlDialect(SparkSqlDialect.DEFAULT_CONTEXT) {

  /**
   * Override the unparse method to handle COLLECTION_TABLE calls specially.
   * Spark SQL doesn't use TABLE(function()) syntax, just function() directly in the FROM clause.
   */
  override def unparseCall(writer: SqlWriter, call: SqlCall, leftPrec: Int, rightPrec: Int): Unit = {
    if (call.getOperator == SqlStdOperatorTable.COLLECTION_TABLE) {
      // For TABLE(function()), just write function() without the TABLE wrapper
      call.operand[SqlCall](0).unparse(writer, leftPrec, rightPrec)
    } else {
      // For all other operators, use the default implementation
      super.unparseCall(writer, call, leftPrec, rightPrec)
    }
  }
}
