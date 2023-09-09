/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.opensearch.flint.core.metadata.FlintMetadata

import org.apache.spark.sql.{DataFrameWriter, Row}
import org.apache.spark.sql.streaming.DataStreamWriter

/**
 * Flint index interface in Spark.
 */
trait FlintSparkIndex {

  /**
   * Index type
   */
  val kind: String

  /**
   * @return
   *   Flint index name
   */
  def name(): String

  /**
   * @return
   *   Flint index metadata
   */
  def metadata(): FlintMetadata

  def buildBatch(flint: FlintSpark): DataFrameWriter[Row]

  def buildStream(flint: FlintSpark): DataStreamWriter[Row]
}

object FlintSparkIndex {

  /**
   * ID column name.
   */
  val ID_COLUMN: String = "__id__"
}
