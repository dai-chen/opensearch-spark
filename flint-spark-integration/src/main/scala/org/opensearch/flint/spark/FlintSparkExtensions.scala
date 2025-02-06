/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.opensearch.flint.spark.function.{ApproxTopKFunction, TumbleFunction}
import org.opensearch.flint.spark.function.topksketch.{AccurateTopKSketch, CountMinSketch, MisraGriesSketch, SpaceSavingSketch}
import org.opensearch.flint.spark.sql.FlintSparkSqlParser

import org.apache.spark.sql.SparkSessionExtensions

/**
 * Flint Spark extension entrypoint.
 */
class FlintSparkExtensions extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectParser { (spark, parser) =>
      new FlintSparkSqlParser(parser)
    }

    extensions.injectFunction(TumbleFunction.description)
    extensions.injectFunction(
      ApproxTopKFunction("approx_top_count_accurate", k => new AccurateTopKSketch(k)))
    extensions.injectFunction(
      ApproxTopKFunction("approx_top_count_misra_gries", k => new MisraGriesSketch(k)))
    extensions.injectFunction(
      ApproxTopKFunction("approx_top_count_cms", k => new CountMinSketch(k)))
    extensions.injectFunction(
      ApproxTopKFunction("approx_top_count_space_saving", k => new SpaceSavingSketch(k)))

    extensions.injectOptimizerRule { spark =>
      new FlintSparkOptimizer(spark)
    }
  }
}
