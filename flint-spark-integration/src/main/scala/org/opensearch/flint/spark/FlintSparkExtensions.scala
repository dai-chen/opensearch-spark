/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.opensearch.common.geo.GeoPoint
import org.opensearch.flint.spark.function.TumbleFunction
import org.opensearch.flint.spark.query.UnifiedQueryParser
import org.opensearch.flint.spark.query.analyzer.{ResolveCalciteFunctions, SafeCalciteFunctionRegistration}
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

    // Register Calcite-backed functions safely (avoiding conflicts with Spark built-ins)
    val registrationConfig = SafeCalciteFunctionRegistration.RegistrationConfig(
      skipBuiltinConflicts = true, // Skip registration if function exists in Spark built-ins
      usePrefixOnConflict = false, // Don't use prefix, just skip
      forceRegister = Set.empty // Can add function names here to force registration
    )

    // Get safe function descriptions and register them
    val safeDescriptions = SafeCalciteFunctionRegistration.getSafeDescriptions(registrationConfig)
    safeDescriptions.foreach { description =>
      extensions.injectFunction(description)
    }

    // Log registration report for debugging
    val report = SafeCalciteFunctionRegistration.getRegistrationReport(registrationConfig)
    logInfo(report.toString)

    // Inject ResolveCalciteFunctions analyzer rule as fallback
    // This acts as a safety net for any functions not in the registry
    extensions.injectResolutionRule { session =>
      ResolveCalciteFunctions(session)
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
