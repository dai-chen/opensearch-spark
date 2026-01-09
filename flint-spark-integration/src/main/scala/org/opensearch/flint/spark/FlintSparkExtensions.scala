/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.opensearch.common.geo.GeoPoint
import org.opensearch.flint.spark.function.TumbleFunction
import org.opensearch.flint.spark.query.UnifiedQuerySparkParser
import org.opensearch.flint.spark.query.api.UnifiedFunctionRepository
import org.opensearch.flint.spark.sql.FlintSparkSqlParser
import org.opensearch.flint.spark.udt.{IPAddress, IPAddressUDT}
import org.opensearch.flint.spark.udt.GeoPointUDT

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.types.UDTRegistration

/**
 * Flint Spark extension entrypoint.
 */
class FlintSparkExtensions extends (SparkSessionExtensions => Unit) with Logging {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    // Inject UnifiedQuerySparkParser to handle PPL queries
    extensions.injectParser { (spark, parser) =>
      new UnifiedQuerySparkParser(spark, new FlintSparkSqlParser(parser))
    }

    // Register regular functions
    UnifiedFunctionRepository.loadFunctions().foreach { description =>
      extensions.injectFunction(description)
    }

    // Register aggregate functions
    UnifiedFunctionRepository.loadAggregateFunctions().foreach { description =>
      extensions.injectFunction(description)
    }

    extensions.injectFunction(TumbleFunction.description)

    extensions.injectOptimizerRule { spark =>
      new FlintSparkOptimizer(spark)
    }

    // Register UDTs
    UDTRegistration.register(classOf[IPAddress].getName, classOf[IPAddressUDT].getName)
    UDTRegistration.register(classOf[GeoPoint].getName, classOf[GeoPointUDT].getName)
  }
}
