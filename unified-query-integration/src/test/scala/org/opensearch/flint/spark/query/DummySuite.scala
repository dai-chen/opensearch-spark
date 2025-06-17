/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query

import scala.collection.JavaConverters._

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.schema.{Schema, Table}
import org.apache.calcite.schema.impl.{AbstractSchema, AbstractTable}
import org.apache.calcite.sql.`type`.SqlTypeName
import org.opensearch.sql.api.UnifiedQueryPlanner
import org.opensearch.sql.executor.QueryType
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class DummySuite extends AnyFlatSpec with BeforeAndAfter {

  private val testSchema: Schema =
    new AbstractSchema {
      override protected def getTableMap: java.util.Map[String, Table] = {
        Map("test" -> (new AbstractTable {
          override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
            val intType = typeFactory.createSqlType(SqlTypeName.INTEGER)
            val stringType = typeFactory.createSqlType(SqlTypeName.VARCHAR)
            typeFactory.createStructType(
              List(intType, stringType).asJava,
              List("id", "name").asJava)
          }
        }: Table)).asJava
      }
    }

  "UnifiedQueryPlanner" should "generate a non-null plan for simple PPL query" in {
    val planner =
      UnifiedQueryPlanner
        .builder()
        .language(QueryType.PPL)
        .catalog("catalog", Map("default" -> testSchema).asJava)
        .build()

    val relNode: RelNode = planner.plan("source = catalog.default.test | eval f = abs(id)")
    assert(relNode != null)
  }
}
