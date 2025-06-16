/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query

import scala.collection.JavaConverters._

import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.schema.{SchemaPlus, Table}
import org.apache.calcite.schema.impl.{AbstractSchema, AbstractTable}
import org.apache.calcite.sql.`type`.SqlTypeName
import org.opensearch.sql.api.UnifiedQueryPlanner
import org.opensearch.sql.executor.QueryType
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class DummySuite extends AnyFlatSpec with BeforeAndAfter {

  private var planner: UnifiedQueryPlanner = _

  before {
    val rootSchema: SchemaPlus = CalciteSchema.createRootSchema(true, false).plus()
    rootSchema.add(
      "catalog",
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
      })

    planner = new UnifiedQueryPlanner(QueryType.PPL, rootSchema)
  }

  "UnifiedQueryPlanner" should "generate a non-null plan for simple PPL query" in {
    val relNode: RelNode = planner.plan("source = catalog.test | eval f = abs(123)")
    assert(relNode != null)
  }
}
