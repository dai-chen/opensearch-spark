/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query.analyzer

import java.util.Locale

import scala.util.Try

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo

/**
 * Provides safe registration of Calcite functions that avoids overriding Spark SQL built-ins.
 *
 * This utility checks if a function already exists in Spark's built-in function registry before
 * registering the Calcite version, ensuring we don't accidentally override standard Spark SQL
 * functions.
 */
object SafeCalciteFunctionRegistration {

  /**
   * Configuration for function registration behavior.
   */
  case class RegistrationConfig(
      // If true, skip registration for functions that exist in Spark's built-in registry
      skipBuiltinConflicts: Boolean = true,
      // If true, register with a prefix (e.g., "ppl_json_delete") when conflict detected
      usePrefixOnConflict: Boolean = false,
      // Prefix to use when usePrefixOnConflict is true
      conflictPrefix: String = "ppl_",
      // List of functions to force-register even if they conflict
      forceRegister: Set[String] = Set.empty)

  /**
   * Check if a function exists in Spark's built-in function registry.
   *
   * Note: This only checks built-in functions, not session-level temporary functions or catalog
   * functions, as those don't exist at extension registration time.
   */
  def isSparkBuiltinFunction(functionName: String): Boolean = {
    // Access Spark's built-in function registry
    // FunctionRegistry.expressions is a Map[String, (ExpressionInfo, FunctionBuilder)]
    // scalastyle:off caselocale
    Try {
      FunctionRegistry.expressions.contains(functionName.toLowerCase(Locale.ROOT))
    }.getOrElse(false)
    // scalastyle:on caselocale
  }

  /**
   * Check if a function exists in the session's function registry at runtime. This includes
   * built-in, temporary, and catalog functions.
   */
  def isRegisteredInSession(spark: SparkSession, functionName: String): Boolean = {
    Try {
      val functionRegistry = spark.sessionState.functionRegistry
      functionRegistry.lookupFunction(FunctionIdentifier(functionName)).isDefined
    }.getOrElse(false)
  }

  /**
   * Get safe function descriptions that won't conflict with Spark built-ins.
   *
   * This method filters CalciteFunctionRegistration.descriptions based on the provided
   * configuration, ensuring we only register functions that are safe.
   */
  def getSafeDescriptions(config: RegistrationConfig = RegistrationConfig())
      : Seq[(FunctionIdentifier, ExpressionInfo, FunctionRegistry.FunctionBuilder)] = {

    CalciteFunctionRegistration.descriptions.flatMap { case (identifier, info, builder) =>
      val functionName = identifier.funcName
      val isBuiltin = isSparkBuiltinFunction(functionName)
      // scalastyle:off caselocale
      val shouldForceRegister =
        config.forceRegister.contains(functionName.toLowerCase(Locale.ROOT))
      // scalastyle:on caselocale

      if (shouldForceRegister) {
        // Force registration - use original name
        Some((identifier, info, builder))
      } else if (isBuiltin && config.skipBuiltinConflicts) {
        if (config.usePrefixOnConflict) {
          // Register with prefix to avoid conflict
          val prefixedName = config.conflictPrefix + functionName
          val newIdentifier = FunctionIdentifier(prefixedName, identifier.database)
          // Create new ExpressionInfo with prefixed name (use simple constructor)
          val newInfo = new ExpressionInfo(info.getClassName, prefixedName)
          Some((newIdentifier, newInfo, builder))
        } else {
          // Skip registration - would conflict with Spark built-in
          None
        }
      } else {
        // Safe to register with original name
        Some((identifier, info, builder))
      }
    }
  }

  /**
   * Get a report of which functions would be registered and which would be skipped. Useful for
   * logging and debugging.
   */
  def getRegistrationReport(
      config: RegistrationConfig = RegistrationConfig()): RegistrationReport = {
    val allFunctions = CalciteFunctionRegistration.descriptions.map(_._1.funcName)
    val builtinConflicts = allFunctions.filter(isSparkBuiltinFunction)
    // scalastyle:off caselocale
    val forceRegistered =
      builtinConflicts.filter(f => config.forceRegister.contains(f.toLowerCase(Locale.ROOT)))
    val skipped = if (config.skipBuiltinConflicts) {
      builtinConflicts.filterNot(f => config.forceRegister.contains(f.toLowerCase(Locale.ROOT)))
    } else {
      Seq.empty
    }
    // scalastyle:on caselocale
    val prefixed = if (config.usePrefixOnConflict) {
      skipped.map(config.conflictPrefix + _)
    } else {
      Seq.empty
    }
    val registered = allFunctions.filterNot(skipped.contains) ++ prefixed.map(
      _.stripPrefix(config.conflictPrefix))

    RegistrationReport(
      totalFunctions = allFunctions.size,
      registered = registered,
      skipped = skipped,
      prefixed = prefixed,
      forceRegistered = forceRegistered,
      conflicts = builtinConflicts)
  }

  /**
   * Report of registration decisions.
   */
  case class RegistrationReport(
      totalFunctions: Int,
      registered: Seq[String],
      skipped: Seq[String],
      prefixed: Seq[String],
      forceRegistered: Seq[String],
      conflicts: Seq[String]) {

    override def toString: String = {
      val sb = new StringBuilder
      sb.append(s"Calcite Function Registration Report:\n")
      sb.append(s"  Total Calcite functions: $totalFunctions\n")
      sb.append(s"  Conflicts with Spark built-ins: ${conflicts.size}\n")
      if (conflicts.nonEmpty) {
        sb.append(s"    Conflicting: ${conflicts.mkString(", ")}\n")
      }
      if (skipped.nonEmpty) {
        sb.append(s"  Skipped (conflicts): ${skipped.size}\n")
        sb.append(s"    Skipped: ${skipped.mkString(", ")}\n")
      }
      if (prefixed.nonEmpty) {
        sb.append(s"  Registered with prefix: ${prefixed.size}\n")
        sb.append(s"    Prefixed: ${prefixed.mkString(", ")}\n")
      }
      if (forceRegistered.nonEmpty) {
        sb.append(s"  Force registered (overriding Spark): ${forceRegistered.size}\n")
        sb.append(s"    Forced: ${forceRegistered.mkString(", ")}\n")
      }
      sb.append(s"  Successfully registered: ${registered.size - forceRegistered.size}\n")
      sb.toString()
    }
  }
}
