/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query.api

import org.apache.calcite.rel.`type`.RelDataType

/**
 * A unified abstraction over execution engines that can evaluate functions using a common
 * contract across Spark and Calcite.
 */
trait UnifiedFunction extends Serializable {

  /** Calcite data types expected for the input arguments. */
  def inputTypes: Seq[RelDataType]

  /** Calcite return type of the function result. */
  def returnType: RelDataType

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
