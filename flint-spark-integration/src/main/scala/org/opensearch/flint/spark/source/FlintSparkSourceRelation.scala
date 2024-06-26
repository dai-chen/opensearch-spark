/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.source

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * This source relation abstraction allows Flint to interact uniformly with different kinds of
 * source data formats (like Spark built-in File, Delta table, Iceberg, etc.), hiding the
 * specifics of each data source implementation.
 */
trait FlintSparkSourceRelation {

  /**
   * @return
   *   the concrete logical plan of the relation associated
   */
  def plan: LogicalPlan

  /**
   * @return
   *   fully qualified table name represented by the relation
   */
  def tableName: String

  /**
   * @return
   *   output column list of the relation
   */
  def output: Seq[AttributeReference]
}
