/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.calcite

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.{RelNode, RelVisitor}
import org.apache.calcite.rel.core.{Filter, Project}
import org.apache.calcite.rex.{RexBuilder, RexCall, RexNode, RexShuttle}
import org.apache.calcite.sql.{SqlFunction, SqlFunctionCategory, SqlKind}
import org.apache.calcite.sql.`type`.{ReturnTypes, OperandTypes}

import scala.collection.JavaConverters._
import java.util.Locale

/**
 * Rule that transforms specific RexCalls to Spark UDF calls.
 */
class CalciteFunctionToSparkUDFRule extends RelOptRule(
  RelOptRule.operand(classOf[RelNode], RelOptRule.none()),
  "CalciteFunctionToSparkUdfRule"
) {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val node = call.rel(0).asInstanceOf[RelNode]
    val rexBuilder = node.getCluster.getRexBuilder

    // Create the transformer
    val transformer = new RexCallTransformer(rexBuilder)

    // Apply the transformer to the RelNode
    val transformedNode = transformRelNode(node, transformer)

    // Only transform if changes were made
    if (transformedNode != node) {
      call.transformTo(transformedNode)
    }
  }

  /**
   * Apply the RexShuttle to all expressions in a RelNode.
   */
  private def transformRelNode(rel: RelNode, shuttle: RexShuttle): RelNode = {
    // Use a collector to find and transform nodes
    val collector = new RelNodeTransformer(shuttle)
    collector.go(rel)
    collector.result
  }
}

/**
 * RexShuttle that transforms specific function calls to Spark UDFs.
 */
class RexCallTransformer(rexBuilder: RexBuilder) extends RexShuttle {

  override def visitCall(call: RexCall): RexNode = {
    val funcName = call.getOperator.getName.toLowerCase(Locale.ROOT)

    // Check if this is a function we want to transform
    if (shouldTransformFunction(funcName)) {
      // Transform operands first
      val transformedOperands = call.getOperands.asScala.map(_.accept(this)).asJava

      // Create a new operator for the Spark UDF with proper return type inference
      val sparkUdfName = "spark_udf_111"
      val sparkUdfOp = new SqlFunction(
        sparkUdfName,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(call.getType),  // Use ReturnTypes.explicit
        null,
        OperandTypes.ANY,  // Accept any operands
        SqlFunctionCategory.USER_DEFINED_FUNCTION
      )

      // Create a new RexCall with the UDF operator
      rexBuilder.makeCall(sparkUdfOp, transformedOperands)
    } else {
      // For other functions, just transform the operands
      super.visitCall(call)
    }
  }

  /**
   * Determine if a function should be transformed to a Spark UDF.
   */
  private def shouldTransformFunction(funcName: String): Boolean = {
    // Add all the function names you want to transform
    true
  }
}

/**
 * Visitor that applies a RexShuttle to all expressions in a RelNode.
 */
class RelNodeTransformer(shuttle: RexShuttle) extends RelVisitor {
  private var root: RelNode = _
  private var transformedRoot: RelNode = _

  /**
   * Get the transformed result.
   */
  def result: RelNode = transformedRoot

  override def visit(rel: RelNode, ordinal: Int, parent: RelNode): Unit = {
    // Store the root node on the first visit
    if (root == null) {
      root = rel
    }

    // Visit children first (depth-first traversal)
    rel.childrenAccept(this)

    // Transform this node
    val transformed = rel match {
      case project: Project =>
        val newProjs = project.getProjects.asScala.map(_.accept(shuttle)).asJava
        if (newProjs != project.getProjects) {
          project.copy(project.getTraitSet, project.getInput, newProjs, project.getRowType)
        } else {
          project
        }

      case filter: Filter =>
        val newCondition = filter.getCondition.accept(shuttle)
        if (newCondition != filter.getCondition) {
          filter.copy(filter.getTraitSet, filter.getInput, newCondition)
        } else {
          filter
        }

      // Add cases for other RelNode types as needed
      // For example: Join, Aggregate, etc.

      case _ => rel // Leave other node types unchanged
    }

    // If this is the root node, save the transformed result
    if (rel == root) {
      transformedRoot = transformed
    }

    // Update parent's reference if this node was transformed
    if (parent != null && transformed != rel) {
      parent.replaceInput(ordinal, transformed)
    }
  }
}
