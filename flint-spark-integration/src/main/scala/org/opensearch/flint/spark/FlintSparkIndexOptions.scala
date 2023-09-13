/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

/**
 * Flint Spark index configurable options.
 *
 * @param options
 *   index option mappings
 */
case class FlintSparkIndexOptions(options: Map[String, String]) {

  def autoRefresh(): Boolean = options.getOrElse("auto_refresh", "false").toBoolean

  def autoStart(): Boolean = options.getOrElse("auto_start", "false").toBoolean

  def refreshInterval(): Option[String] = options.get("refresh_interval")

  def checkpointLocation(): Option[String] = options.get("checkpoint_location")

  def indexSettings(): Option[String] = options.get("index_settings")
}

object FlintSparkIndexOptions {

  val empty = FlintSparkIndexOptions(Map.empty)
}
