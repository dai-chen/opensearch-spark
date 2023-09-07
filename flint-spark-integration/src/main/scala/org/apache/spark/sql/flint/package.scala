/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

package object flint {

  def dataFrameToLogicalPlan(df: DataFrame): LogicalPlan = {
    df.logicalPlan
  }

  def logicalPlanToDataFrame(spark: SparkSession, logicalPlan: LogicalPlan): DataFrame = {
    Dataset.ofRows(spark, logicalPlan)
  }
}
