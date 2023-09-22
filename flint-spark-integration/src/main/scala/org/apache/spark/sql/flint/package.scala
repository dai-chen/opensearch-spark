/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

package object flint {

  /**
   * Extract Catalyst logical plan in Spark DataFrame.
   *
   * @param df
   *   data frame
   * @return
   *   logical plan
   */
  def dataFrameToLogicalPlan(df: DataFrame): LogicalPlan = {
    df.logicalPlan
  }

  /**
   * Construct Spark DataFrame from Catalyst logical plan.
   *
   * @param spark
   *   spark session
   * @param logicalPlan
   *   logical plan
   * @return
   *   data frame
   */
  def logicalPlanToDataFrame(spark: SparkSession, logicalPlan: LogicalPlan): DataFrame = {
    Dataset.ofRows(spark, logicalPlan)
  }
}
