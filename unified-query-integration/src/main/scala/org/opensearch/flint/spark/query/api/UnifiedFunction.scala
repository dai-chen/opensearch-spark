/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query.api

/**
 * A unified abstraction over execution engines that can evaluate functions using a common
 * contract across Spark and Calcite.
 */
trait UnifiedFunction extends Serializable {

  val functionName: String

  /** SQL type names expected for the input arguments. */
  def inputTypes: Seq[String]

  /** SQL type name of the function result. */
  def returnType: String

  /** Whether this function can return null values. */
  def nullable: Boolean

  /**
   * Interpret the function with already evaluated inputs.
   *
   * @param inputs
   *   Argument values evaluated by the caller.
   * @return
   *   The evaluated result.
   */
  def eval(inputs: Seq[Any]): Any
}
