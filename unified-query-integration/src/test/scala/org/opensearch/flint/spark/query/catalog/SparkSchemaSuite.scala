/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query.catalog

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class SparkSchemaSuite extends SharedSparkSession {

  override protected def sparkConf = {
    new SparkConf()
      .set("spark.ui.enabled", "false")
      .set(SQLConf.CODEGEN_FALLBACK.key, "false")
      .set(SQLConf.CODEGEN_FACTORY_MODE.key, CodegenObjectFactoryMode.CODEGEN_ONLY.toString)
    // Disable ConvertToLocalRelation for better test coverage. Test cases built on
    // LocalRelation will exercise the optimization rules better by disabling it as
    // this rule may potentially block testing of other optimization rules such as
    // ConstantPropagation etc.
    // Register your dev catalog
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    sql(s"CREATE TABLE spark_catalog.default.t1 (name STRING, age INT) USING JSON")
  }

  test("test") {
    sql("CREATE DATABASE test")
    withDatabase("test") {
      sql(s"CREATE TABLE spark_catalog.test.t2 (name STRING, age INT) USING JSON")

      val schema = new SparkSchema(spark, "spark_catalog")
      val t1 = schema.getSubSchema("default").getTable("t1")
      val t2 = schema.getSubSchema("test").getTable("t2")
    }
  }
}
