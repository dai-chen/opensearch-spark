/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.opensearch.common.geo.GeoPoint
import org.opensearch.flint.spark.function.TumbleFunction
import org.opensearch.flint.spark.query.UnifiedQueryParser
import org.opensearch.flint.spark.query.analyzer.SafeCalciteFunctionRegistration
import org.opensearch.flint.spark.query.calcite.CalciteExecutionContext
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
    // Inject UnifiedQueryParser to handle PPL queries
    extensions.injectParser { (spark, parser) =>
      new UnifiedQueryParser(spark, new FlintSparkSqlParser(parser))
    }

    // Get safe function descriptions and register them
    val calciteContext = CalciteExecutionContext.getOrCreate()
    val safeDescriptions =
      SafeCalciteFunctionRegistration.getSafeDescriptions(calciteContext)
    safeDescriptions.foreach { description =>
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
