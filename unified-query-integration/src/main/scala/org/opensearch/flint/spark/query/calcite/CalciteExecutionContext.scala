/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query.calcite

import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.jdbc.JavaTypeFactoryImpl
import org.apache.calcite.rex.RexBuilder

/**
 * Manages Calcite execution context for creating Calcite types and RexNodes. This context
 * provides:
 *   - Type factory for creating Calcite types
 *   - RexBuilder for creating RexNodes
 */
class CalciteExecutionContext extends Serializable {

  @transient private lazy val typeFactory: JavaTypeFactory = new JavaTypeFactoryImpl()

  @transient private lazy val rexBuilder: RexBuilder = new RexBuilder(typeFactory)

  def getTypeFactory: JavaTypeFactory = typeFactory

  def getRexBuilder: RexBuilder = rexBuilder
}

/**
 * Companion object for managing shared CalciteExecutionContext instances.
 */
object CalciteExecutionContext {

  /**
   * Get a shared CalciteExecutionContext instance. This is thread-safe and can be reused across
   * multiple queries.
   */
  def getOrCreate(): CalciteExecutionContext = {
    new CalciteExecutionContext()
  }
}
