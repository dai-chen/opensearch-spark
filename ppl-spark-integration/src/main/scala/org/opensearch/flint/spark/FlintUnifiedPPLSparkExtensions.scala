/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.opensearch.flint.spark.query.{SparkSchema, UnifiedQuerySparkParser}
import org.opensearch.sql.api.UnifiedQueryContext
import org.opensearch.sql.executor.QueryType

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}

/**
 * Flint Unified PPL Spark extension entrypoint that injects unified PPL parser and functions.
 */
class FlintUnifiedPPLSparkExtensions extends (SparkSessionExtensions => Unit) with Logging {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectParser { (spark, parser) =>
      // Pass context as by-name parameter to defer evaluation until first query
      // This avoids accessing spark.catalog during SparkSession initialization
      new UnifiedQuerySparkParser(buildContext(spark), parser)
    }
  }

  private def buildContext(spark: SparkSession): UnifiedQueryContext = {
    val currentCatalog = spark.catalog.currentCatalog
    val currentDatabase = spark.catalog.currentDatabase
    val catalogNames = spark.sessionState.catalogManager.listCatalogs(None).toSet + currentCatalog

    UnifiedQueryContext
      .builder()
      .language(QueryType.PPL)
      // .cacheMetadata(true)
      .settings(
        "plugins.calcite.all_join_types.allowed" -> true,
        "plugins.ppl.subsearch.maxout" -> 0,
        "plugins.ppl.join.subsearch_maxout" -> 0)
      .catalogs { b =>
        catalogNames.foreach(name => {
          logInfo(s"Registering Spark catalog $name")
          b.catalog(name, new SparkSchema(spark, name))
        })
      }
      .defaultNamespace(s"$currentCatalog.$currentDatabase")
      .build()
  }

  private implicit class UnifiedQueryContextBuilderOps(builder: UnifiedQueryContext.Builder) {

    def catalogs(block: UnifiedQueryContext.Builder => Unit): UnifiedQueryContext.Builder = {
      block(builder)
      builder
    }

    def settings(entries: (String, Any)*): UnifiedQueryContext.Builder = {
      entries.foreach { case (key, value) => builder.setting(key, value) }
      builder
    }
  }
}
