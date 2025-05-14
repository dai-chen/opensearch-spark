/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.opensearch.flint.spark.function.TumbleFunction
import org.opensearch.flint.spark.sql.FlintSparkSqlParser
import org.opensearch.flint.spark.udt.{IPAddress, IPAddressUDT}
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.UDTRegistration
import org.opensearch.flint.spark.calcite.{OpenSearchQueryFunction, OpenSearchTableValueFunctions}

/**
 * Flint Spark extension entrypoint.
 */
class FlintSparkExtensions extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectParser { (spark, parser) =>
      new FlintSparkSqlParser(parser)
    }
    extensions.injectParser { (spark, parser) =>
      new FlintSparkPPLCalciteParser(spark, parser)
    }

    extensions.injectFunction(TumbleFunction.description)

    extensions.injectOptimizerRule { spark =>
      new FlintSparkOptimizer(spark)
    }

    // Register UDTs
    UDTRegistration.register(classOf[IPAddress].getName, classOf[IPAddressUDT].getName)

    // Register UDTFs
    val fnInfo = OpenSearchTableValueFunctions.getTableValueFunctionInjection(
      OpenSearchTableValueFunctions.OPENSEARCH_QUERY)
    extensions.injectTableFunction(fnInfo._1, fnInfo._2, fnInfo._3)

    // Add resolver rule to convert the function to relation
    extensions.injectResolutionRule(session => OpenSearchTableFunctionResolver(session))
  }

  /**
   * Resolution rule to convert OpenSearch table functions to relations
   */
  case class OpenSearchTableFunctionResolver(spark: SparkSession)
    extends Rule[LogicalPlan] {

    override def apply(plan: LogicalPlan): LogicalPlan = {
      plan.resolveOperatorsDown {
        case f: OpenSearchQueryFunction if f.functionArgs.forall(_.resolved) =>
          // Create an unresolved relation with original expressions attached
          f.toRelation(spark)
      }
    }
  }
}
