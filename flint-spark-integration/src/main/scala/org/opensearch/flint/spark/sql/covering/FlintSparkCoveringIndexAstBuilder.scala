/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.sql.covering

import org.opensearch.flint.spark.FlintSpark.RefreshMode
import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex.getFlintIndexName
import org.opensearch.flint.spark.sql.{FlintSparkSqlCommand, FlintSparkSqlExtensionsVisitor}
import org.opensearch.flint.spark.sql.FlintSparkSqlAstBuilder.{getFullTableName, isAutoRefreshEnabled}
import org.opensearch.flint.spark.sql.FlintSparkSqlExtensionsParser.{CreateCoveringIndexStatementContext, DropCoveringIndexStatementContext, RefreshCoveringIndexStatementContext}

import org.apache.spark.sql.catalyst.plans.logical.Command

/**
 * Flint Spark AST builder that builds Spark command for Flint covering index statement.
 */
trait FlintSparkCoveringIndexAstBuilder extends FlintSparkSqlExtensionsVisitor[AnyRef] {

  override def visitCreateCoveringIndexStatement(
      ctx: CreateCoveringIndexStatementContext): Command = {
    FlintSparkSqlCommand() { flint =>
      val indexName = ctx.indexName.getText
      val tableName = ctx.tableName.getText
      val indexBuilder =
        flint
          .coveringIndex()
          .name(indexName)
          .onTable(tableName)

      ctx.indexColumns.multipartIdentifierProperty().forEach { indexColCtx =>
        val colName = indexColCtx.multipartIdentifier().getText
        indexBuilder.addIndexColumns(colName)
      }
      indexBuilder.create()

      // Trigger auto refresh if enabled
      if (isAutoRefreshEnabled(ctx.propertyList())) {
        val flintIndexName = getFlintIndexName(indexName, getFullTableName(flint, ctx.tableName))
        flint.refreshIndex(flintIndexName, RefreshMode.INCREMENTAL)
      }
      Seq.empty
    }
  }

  override def visitRefreshCoveringIndexStatement(
      ctx: RefreshCoveringIndexStatementContext): Command = {
    FlintSparkSqlCommand() { flint =>
      val indexName = ctx.indexName.getText
      val tableName = ctx.tableName.getText
      val flintIndexName = getFlintIndexName(indexName, tableName)

      flint.refreshIndex(flintIndexName, RefreshMode.FULL)
      Seq.empty
    }
  }

  override def visitDropCoveringIndexStatement(
      ctx: DropCoveringIndexStatementContext): Command = {
    FlintSparkSqlCommand() { flint =>
      val indexName = ctx.indexName.getText
      val tableName = getFullTableName(flint, ctx.tableName)
      val flintIndexName = getFlintIndexName(indexName, tableName)

      flint.deleteIndex(flintIndexName)
      Seq.empty
    }
  }
}
