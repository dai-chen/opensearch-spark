/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query.calcite

import scala.collection.JavaConverters._

import org.apache.calcite.DataContext
import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.jdbc.JavaTypeFactoryImpl
import org.apache.calcite.rex.{RexBuilder, RexExecutorImpl, RexNode}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.DataType

/**
 * Manages Calcite execution context for evaluating RexNode expressions using RexExecutorImpl.
 * This context provides:
 *   - Type factory for creating Calcite types
 *   - RexBuilder for creating RexNodes
 *   - Compiled execution via RexExecutorImpl which uses JaninoRexCompiler for code generation
 */
class CalciteExecutionContext extends Logging with Serializable {

  @transient private lazy val typeFactory: JavaTypeFactory = new JavaTypeFactoryImpl()

  @transient private lazy val rexBuilder: RexBuilder = new RexBuilder(typeFactory)

  /**
   * Evaluate a RexNode using RexExecutorImpl which compiles RexNodes to Java bytecode.
   * RexExecutorImpl internally uses JaninoRexCompiler for code generation.
   */
  def evaluate(rexNode: RexNode, inputValues: Seq[Any], resultDataType: DataType): Any = {
    try {
      // Create a DataContext that provides input values
      val dataContext = new SimpleDataContext(inputValues, typeFactory)

      // Create RexExecutorImpl which uses Janino compiler for code generation
      val rexExecutor = new RexExecutorImpl(dataContext)

      // Create a mutable list for the reduction
      val exprList = new java.util.ArrayList[RexNode]()
      exprList.add(rexNode)

      // Reduce/evaluate the RexNode using the compiled code
      // This will generate Java code, compile it, and execute it
      // The reduce method modifies the list in place
      rexExecutor.reduce(rexBuilder, exprList, null)

      // Get the result - reduced expressions contain the evaluated result
      val result = if (exprList != null && exprList.size() > 0) {
        val reducedNode = exprList.get(0)
        // If the result is a literal, extract its value
        if (reducedNode.isInstanceOf[org.apache.calcite.rex.RexLiteral]) {
          val literal = reducedNode.asInstanceOf[org.apache.calcite.rex.RexLiteral]
          literal.getValue
        } else {
          // If still not a literal, something went wrong
          throw new RuntimeException(
            s"Expected literal result after reduction, got: ${reducedNode.getClass}")
        }
      } else {
        null
      }

      // Convert result back to Spark format
      CalciteTypeConverter.calciteToSparkValue(result, resultDataType)
    } catch {
      case e: Exception =>
        logError(s"Failed to evaluate RexNode: ${rexNode.toString}", e)
        throw new RuntimeException(s"Failed to evaluate Calcite expression: ${e.getMessage}", e)
    }
  }

  def getTypeFactory: JavaTypeFactory = typeFactory

  def getRexBuilder: RexBuilder = rexBuilder
}

/**
 * Simple DataContext implementation for providing input values to Calcite's RexExecutor.
 */
class SimpleDataContext(inputValues: Seq[Any], override val getTypeFactory: JavaTypeFactory)
    extends DataContext {

  override def get(name: String): AnyRef = {
    // Parse input reference from name (e.g., "$0", "$1")
    if (name.startsWith("$")) {
      val index = name.substring(1).toInt
      if (index < inputValues.size) {
        val value = inputValues(index)
        if (value == null) null else value.asInstanceOf[AnyRef]
      } else {
        null
      }
    } else {
      null
    }
  }

  override def getRootSchema: org.apache.calcite.schema.SchemaPlus = null

  override def getQueryProvider: org.apache.calcite.linq4j.QueryProvider = null
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
