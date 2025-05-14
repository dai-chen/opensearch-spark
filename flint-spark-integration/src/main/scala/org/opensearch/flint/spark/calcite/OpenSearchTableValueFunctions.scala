/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.calcite

import scala.collection.JavaConverters._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistryBase, TableFunctionRegistry, UnresolvedLeafNode, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, ExpressionInfo, Literal, StringLiteral}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Registry for OpenSearch table-valued functions
 */
object OpenSearchTableValueFunctions {
  val OPENSEARCH_QUERY = "opensearch_query"
  val supportedFnNames = Seq(OPENSEARCH_QUERY)

  // For use with SparkSessionExtensions
  type TableFunctionDescription =
    (FunctionIdentifier, ExpressionInfo, TableFunctionRegistry.TableFunctionBuilder)

  /**
   * Get the TableFunctionDescription to be injected in SparkSessionExtension
   */
  def getTableValueFunctionInjection(fnName: String): TableFunctionDescription = {
    val (info, builder) = fnName match {
      case OPENSEARCH_QUERY =>
        FunctionRegistryBase.build[OpenSearchQueryFunction](fnName, since = None)
      case _ => throw new IllegalArgumentException(s"Unsupported function: $fnName")
    }
    val ident = FunctionIdentifier(fnName)
    (ident, info, builder)
  }
}

/**
 * Base trait for OpenSearch table value functions
 */
trait OpenSearchTableValueFunction extends UnresolvedLeafNode {
  def fnName: String
  val functionArgs: Seq[Expression]
}

/**
 * Represents the OPENSEARCH_QUERY table function
 * Usage: SELECT * FROM OPENSEARCH_QUERY('indexName', 'queryDSL')
 */
case class OpenSearchQueryFunction(override val functionArgs: Seq[Expression])
  extends OpenSearchTableValueFunction {

  override def fnName: String = OpenSearchTableValueFunctions.OPENSEARCH_QUERY

  // Constructor for reflective instantiation
  def this() = this(Nil)

  // Validate arguments
  if (functionArgs.size < 2) {
    throw new IllegalArgumentException(
      s"Not enough arguments for $fnName. Expected: indexName, queryDSL")
  }
  if (functionArgs.size > 2) {
    throw new IllegalArgumentException(
      s"Too many arguments for $fnName. Expected: indexName, queryDSL")
  }

  /**
   * Convert this function to an unresolved relation with the DSL in options
   */
  def toRelation(spark: SparkSession): LogicalPlan = {
    // Get index name
    val indexNameExpr = functionArgs.head
    val indexName = indexNameExpr match {
      case StringLiteral(value) => value
      case _ => throw new IllegalArgumentException(
        "Index name must be a string literal")
    }

    // Get DSL query
    val dslExpr = functionArgs(1)
    val dsl = dslExpr match {
      case StringLiteral(value) => value
      case _ => throw new IllegalArgumentException(
        "Query DSL must be a string literal")
    }

    // Create options with the DSL
    val options = Map(
      "index" -> indexName,
      "dsl" -> dsl
    )

    // Return an unresolved relation with index name and options
    // Your existing data source will handle this later
    UnresolvedRelation(
      indexName.split('.'),
      new CaseInsensitiveStringMap(options.asJava),
      isStreaming = false)
  }

  override def output: Seq[Attribute] = Nil
}
