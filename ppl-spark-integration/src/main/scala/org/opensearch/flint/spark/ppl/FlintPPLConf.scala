/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import java.util.{Map => JMap}

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Configuration settings for Flint PPL Spark integration.
 */
object FlintPPLConf {

  /**
   * Config key to enable the unified (Calcite-based) PPL parser. When false (default), uses the
   * legacy FlintSparkPPLParser. When true, uses the new UnifiedQuerySparkParser.
   */
  val PPL_UNIFIED_ENABLED_KEY = "spark.flint.ppl.unified.enabled"
  val PPL_UNIFIED_ENABLED_DEFAULT = false

  /**
   * Create FlintPPLConf from SparkSession. Reads from runtime config (spark.conf) which can be
   * modified at runtime.
   */
  def apply(spark: SparkSession): FlintPPLConf = {
    new FlintPPLConf(spark.conf.getAll.toMap.asJava)
  }

  /**
   * Create FlintPPLConf from SparkConf. Used during parser injection when only SparkConf is
   * available.
   */
  def apply(conf: SparkConf): FlintPPLConf = {
    new FlintPPLConf(conf.getAll.toMap.asJava)
  }
}

case class FlintPPLConf(properties: JMap[String, String]) {
  import FlintPPLConf._

  def isPPLUnifiedEnabled: Boolean = {
    Option(properties.get(PPL_UNIFIED_ENABLED_KEY))
      .map(_.toBoolean)
      .getOrElse(PPL_UNIFIED_ENABLED_DEFAULT)
  }
}
