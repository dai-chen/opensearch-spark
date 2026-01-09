/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.opensearch.flint.spark.query.UnifiedQuerySparkParser
import org.opensearch.flint.spark.query.api.UnifiedFunctionRepository
import org.opensearch.flint.spark.sql.FlintSparkSqlParser

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions

class FlintNewSparkPPLExtensions extends (SparkSessionExtensions => Unit) with Logging {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    // Inject UnifiedQuerySparkParser to handle PPL queries
    extensions.injectParser { (spark, parser) =>
      new UnifiedQuerySparkParser(spark, parser)
    }

    // Register regular functions
    UnifiedFunctionRepository.loadFunctions().foreach { description =>
      extensions.injectFunction(description)
    }

    // Register aggregate functions
    UnifiedFunctionRepository.loadAggregateFunctions().foreach { description =>
      extensions.injectFunction(description)
    }
  }
}
