/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.calcite

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.rel2sql.{RelToSqlConverter, SqlImplementor}
import org.apache.calcite.rel.rel2sql.SqlImplementor.Clause
import org.apache.calcite.sql.`type`.{OperandTypes, ReturnTypes}
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.{SqlBasicCall, SqlDialect, SqlFunction, SqlFunctionCategory, SqlIdentifier, SqlKind, SqlLiteral, SqlNode, SqlNodeList, SqlSelect}
import org.apache.calcite.sql.parser.SqlParserPos
import org.opensearch.sql.opensearch.storage.scan.CalciteEnumerableIndexScan

import scala.collection.JavaConverters._

class CustomRelToSqlConverter(dialect: SqlDialect) extends RelToSqlConverter(dialect) {

  override def visit(scan: TableScan): SqlImplementor#Result = scan match {
    case indexScan: CalciteEnumerableIndexScan => visit(indexScan)
    case _ => super.visit(scan)
  }

  /**
   * Visits a CalciteEnumerableIndexScan and converts it to a call to opensearch_query.
   */
  private def visit(indexScan: CalciteEnumerableIndexScan): SqlImplementor#Result = {
    // Extract index name and query
    val indexName = indexScan.getTable.getQualifiedName.asScala.mkString(".")
    val dslQuery = indexScan.getDslQuery
    val escapedDsl = dslQuery.replace("'", "''")

    // Create a simple function operator for opensearch_query
    val opensearchQueryFunction = new SqlFunction(
      "opensearch_query",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.CURSOR,
      null,
      OperandTypes.STRING_STRING,
      SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION
    )

    // Create the function call using the createCall API
    val functionCall = opensearchQueryFunction.createCall(
      null, // No function qualifier
      SqlParserPos.ZERO,
      SqlLiteral.createCharString(indexName, SqlParserPos.ZERO),
      SqlLiteral.createCharString(escapedDsl, SqlParserPos.ZERO)
    )

    // Wrap in TABLE() operator
    /*
    val tableFunction = SqlStdOperatorTable.COLLECTION_TABLE.createCall(
      SqlParserPos.ZERO,
      functionCall
    )
     */

    // Create a SELECT statement
    val select = new SqlSelect(
      SqlParserPos.ZERO,
      null, // No hints
      SqlNodeList.SINGLETON_STAR, // SELECT *
      //tableFunction, // Spark doesn't need TABLE wrapper like FROM TABLE(opensearch_query(...))
      functionCall,
      null, // No WHERE clause
      null, null, null, null, null, null, null, null // No other clauses
    )

    // Return the result with appropriate clauses
    // result(select, JCollections.singletonList(Clause.FROM), indexScan, null)
    result(select, java.util.Collections.singletonList(Clause.FROM), indexScan, null)
  }
}
