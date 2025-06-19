/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query

import scala.collection.JavaConverters._

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.schema.{Schema, Table}
import org.apache.calcite.schema.impl.{AbstractSchema, AbstractTable}
import org.apache.calcite.sql.`type`.SqlTypeName
import org.opensearch.sql.api.UnifiedQueryPlanner
import org.opensearch.sql.executor.QueryType
import org.scalatest.flatspec.AnyFlatSpec

/**
 * Dummy integration test to verify that UnifiedQueryPlanner can be constructed and used in a
 * Spark context with a basic schema. This test ensures the wiring works and will be removed
 * shortly.
 */
class DummySuite extends AnyFlatSpec {

  private val testSchema: Schema =
    new AbstractSchema {
      override protected def getTableMap: java.util.Map[String, Table] = {
        Map("test" -> (new AbstractTable {
          override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
            typeFactory.createStructType(
              List(typeFactory.createSqlType(SqlTypeName.INTEGER)).asJava,
              List("id").asJava)
          }
        }: Table)).asJava
      }
    }

  "UnifiedQueryPlanner" should "generate a non-null plan for simple PPL query" in {
    val planner =
      UnifiedQueryPlanner
        .builder()
        .language(QueryType.PPL)
        .catalog("catalog", testSchema)
        .cacheSchema(true)
        .build()

    val relNode: RelNode = planner.plan("source = catalog.test | eval f = abs(id)")
    assert(relNode != null)
  }
}
