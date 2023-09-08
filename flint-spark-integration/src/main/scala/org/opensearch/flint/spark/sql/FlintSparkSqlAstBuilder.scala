/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.sql

import scala.collection.JavaConverters.asScalaBufferConverter

import org.antlr.v4.runtime.tree.RuleNode
import org.opensearch.flint.spark.{FlintSpark, FlintSparkIndexOptions}
import org.opensearch.flint.spark.sql.FlintSparkSqlExtensionsParser.PropertyListContext
import org.opensearch.flint.spark.sql.covering.FlintSparkCoveringIndexAstBuilder
import org.opensearch.flint.spark.sql.skipping.FlintSparkSkippingIndexAstBuilder

/**
 * Flint Spark AST builder that builds Spark command for Flint index statement. This class mix-in
 * all other AST builders and provides util methods.
 */
class FlintSparkSqlAstBuilder
    extends FlintSparkSqlExtensionsBaseVisitor[AnyRef]
    with FlintSparkSkippingIndexAstBuilder
    with FlintSparkCoveringIndexAstBuilder
    with FlintSparkSqlAstBuilderBase {

  override def aggregateResult(aggregate: AnyRef, nextResult: AnyRef): AnyRef =
    if (nextResult != null) nextResult else aggregate
}

object FlintSparkSqlAstBuilder {

  /**
   * Check if auto_refresh is true in property list.
   *
   * @param ctx
   *   property list
   */
  def isAutoRefreshEnabled(ctx: PropertyListContext): Boolean = {
    if (ctx == null) {
      false
    } else {
      ctx
        .property()
        .forEach(p => {
          if (p.key.getText == "auto_refresh") {
            return p.value.getText.toBoolean
          }
        })
      false
    }
  }

  /**
   * Get full table name if database not specified.
   *
   * @param flint
   *   Flint Spark which has access to Spark Catalog
   * @param tableNameCtx
   *   table name
   * @return
   */
  def getFullTableName(flint: FlintSpark, tableNameCtx: RuleNode): String = {
    val tableName = tableNameCtx.getText
    if (tableName.contains(".")) {
      tableName
    } else {
      val db = flint.spark.catalog.currentDatabase
      s"$db.$tableName"
    }
  }

  def parseIndexOptions(ctx: PropertyListContext): FlintSparkIndexOptions = {
    val options =
      ctx
        .property()
        .asScala
        .map { p =>
          val key = p.propertyKey().getText
          val value = p.propertyValue().getText
          key -> value
        }
        .toMap
    FlintSparkIndexOptions(options)
  }
}
