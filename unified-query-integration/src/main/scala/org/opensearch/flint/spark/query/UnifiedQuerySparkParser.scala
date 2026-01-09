/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query

import org.apache.calcite.rel.RelNode
import org.opensearch.flint.spark.query.calcite.OpenSearchSparkSqlDialect
import org.opensearch.flint.spark.query.catalog.SparkSchema
import org.opensearch.sql.api.{UnifiedQueryContext, UnifiedQueryPlanner}
import org.opensearch.sql.api.transpiler.UnifiedQueryTranspiler
import org.opensearch.sql.common.antlr.SyntaxCheckException
import org.opensearch.sql.executor.QueryType

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * A custom Spark SQL parser that delegates query parsing and planning to the Unified Query
 * Planner. It converts unified queries into Spark SQL queries for execution, and falls back to
 * the default Spark parser when the input query is not supported.
 *
 * This parser leverages the latest UnifiedQueryContext, UnifiedQueryPlanner, and
 * UnifiedQueryTranspiler APIs from the unified query library (2.19+), which provide a centralized
 * configuration approach for query planning and transpilation.
 */
class UnifiedQuerySparkParser(
    spark: SparkSession,
    sparkParser: ParserInterface,
    queryType: QueryType = QueryType.PPL)
    extends ParserInterface
    with Logging {

  /**
   * Unified query context builder with all registered Spark catalogs. The actual context is built
   * per query to reflect the current catalog and namespace, ensuring consistency with Spark SQL's
   * table resolution behavior.
   */
  private lazy val unifiedQueryContextBuilder: UnifiedQueryContext.Builder = {
    val catalogManager = spark.sessionState.catalogManager
    val builder =
      UnifiedQueryContext
        .builder()
        .language(queryType)

    // TODO: list all catalogs even if not loaded yet
    catalogManager
      .listCatalogs(None)
      .foreach(catalogName => {
        builder.catalog(catalogName, new SparkSchema(spark, catalogName))
      })
    builder
  }

  /** Transpiler that converts unified plan to Spark SQL using Spark SQL dialect. */
  private val sparkSqlTranspiler = UnifiedQueryTranspiler
    .builder()
    .dialect(OpenSearchSparkSqlDialect.DEFAULT)
    .build()

  override def parsePlan(query: String): LogicalPlan = {
    try {
      val context = buildUnifiedQueryContext()
      val planner = new UnifiedQueryPlanner(context)
      val unifiedPlan = planner.plan(query)
      val sqlText = sparkSqlTranspiler.toSql(unifiedPlan)

      logWarning(s"PPL translated to Spark SQL:\n $sqlText \n")
      sparkParser.parsePlan(sqlText)
    } catch {
      // Fall back to Spark parser if unified query planner cannot handle
      case _: SyntaxCheckException => sparkParser.parsePlan(query)
    }
  }

  override def parseExpression(sqlText: String): Expression = sparkParser.parseExpression(sqlText)

  override def parseTableIdentifier(sqlText: String): TableIdentifier =
    sparkParser.parseTableIdentifier(sqlText)

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
    sparkParser.parseFunctionIdentifier(sqlText)

  override def parseMultipartIdentifier(sqlText: String): Seq[String] =
    sparkParser.parseMultipartIdentifier(sqlText)

  override def parseTableSchema(sqlText: String): StructType =
    sparkParser.parseTableSchema(sqlText)

  override def parseDataType(sqlText: String): DataType = sparkParser.parseDataType(sqlText)

  override def parseQuery(sqlText: String): LogicalPlan = sparkParser.parseQuery(sqlText)

  private def buildUnifiedQueryContext(): UnifiedQueryContext = {
    val currentCatalog = spark.catalog.currentCatalog
    val currentDatabase = spark.catalog.currentDatabase
    unifiedQueryContextBuilder
      .defaultNamespace(s"$currentCatalog.$currentDatabase")
      .build()
  }
}
