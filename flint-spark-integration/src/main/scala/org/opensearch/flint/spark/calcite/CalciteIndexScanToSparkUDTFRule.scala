/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.calcite

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.logical.LogicalTableFunctionScan
import org.apache.calcite.sql.{SqlFunction, SqlFunctionCategory, SqlKind}
import org.apache.calcite.sql.`type`.{OperandTypes, ReturnTypes}
import org.opensearch.sql.opensearch.storage.scan.CalciteEnumerableIndexScan

import scala.collection.JavaConverters._
import java.util.{Collections => JCollections}

/**
 * Rule that transforms CalciteEnumerableIndexScan to a table function scan
 * calling opensearch_query.
 */
class CalciteIndexScanToSparkUDTFRule extends RelOptRule(
  RelOptRule.operand(classOf[CalciteEnumerableIndexScan], RelOptRule.none()),
  "OpenSearchIndexScanToTableFunctionRule"
) {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val indexScan = call.rel(0).asInstanceOf[CalciteEnumerableIndexScan]
    val cluster = indexScan.getCluster
    val rexBuilder = cluster.getRexBuilder

    // Extract index name and query
    val indexName = indexScan.getTable.getQualifiedName.asScala.mkString(".")
    val dslQuery = indexScan.getDslQuery

    // Create function operator for opensearch_query
    val opensearchQueryOp = new SqlFunction(
      "opensearch_query",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.CURSOR,
      null,
      OperandTypes.STRING_STRING,
      SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION
    )

    // Create function call using the correct signature
    val functionCall = rexBuilder.makeCall(
      opensearchQueryOp,
      rexBuilder.makeLiteral(indexName),
      rexBuilder.makeLiteral(dslQuery)
    )

    // Create a table function scan node
    val replacement = LogicalTableFunctionScan.create(
      cluster,
      JCollections.emptyList[RelNode](),
      functionCall,
      null, // elementType
      indexScan.getRowType,
      null // columnMappings
    )

    call.transformTo(replacement)
  }
}
