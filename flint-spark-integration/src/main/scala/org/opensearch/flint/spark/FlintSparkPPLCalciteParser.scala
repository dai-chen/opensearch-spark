/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 * This file contains code from the Apache Spark project (original license below).
 * It contains modifications, which are licensed as above:
 */

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opensearch.flint.spark

import scala.collection.JavaConverters._
import scala.collection.JavaConverters.mapAsJavaMapConverter
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory, RelDataTypeField}
import org.apache.calcite.rel.`type`.RelDataTypeFieldImpl
import org.apache.calcite.rel.rel2sql.RelToSqlConverter
import org.apache.calcite.schema.{Schema, Table}
import org.apache.calcite.schema.impl.{AbstractSchema, AbstractTable}
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.dialect.SparkSqlDialect
import org.opensearch.sql.common.antlr.SyntaxCheckException
import org.opensearch.sql.executor.QueryType
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalog.Database
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructType, TimestampType}
import org.opensearch.sql.api.UnifiedQueryPlanner

/**
 * Flint PPL parser that parse PPL Query Language into spark logical plan - if parse fails it will
 * fall back to spark's parser.
 *
 * @param sparkParser
 *   Spark SQL parser
 */
class FlintSparkPPLCalciteParser(val spark: SparkSession, sparkParser: ParserInterface)
    extends ParserInterface
    with Logging {

  private lazy val unifiedQueryPlanner: UnifiedQueryPlanner = {
    UnifiedQueryPlanner
      .builder()
      .language(QueryType.PPL)
      .catalog("spark_catalog",
        spark.catalog
          .listDatabases
          .collect
          .flatMap(db => Some(db.name, new SparkSchema(spark, db).asInstanceOf[Schema]))
          .toMap
          .asJava)
      .build()
  }

  /*
  private lazy val unifiedQueryPlanner: UnifiedQueryPlanner = {
    UnifiedQueryPlanner
      .builder()
      .language(QueryType.PPL)
      .catalogs { root =>
        // TODO: list all catalogs
        val catalogs = Seq("spark_catalog", "dev")
        catalogs.foreach { catalog =>
          val calciteCatalog = root.add(catalog, new AbstractSchema)

          if (catalog == "spark_catalog") {
            spark.catalog.listDatabases.collect.foreach(db => {
              calciteCatalog.add(db.name, new SparkSchema(spark, catalog, db.name))
            })
          } else {
            // OS doesn't implement SupportsNamespace
            calciteCatalog.add("default", new SparkSchema(spark, catalog, "default"))
          }
        }
      }
      .build()
  }
   */

  override def parsePlan(pplText: String): LogicalPlan = {
    try {
      val relNode = unifiedQueryPlanner.plan(pplText)
      val converter = new RelToSqlConverter(SparkSqlDialect.DEFAULT)
      val result = converter.visitRoot(relNode)
      val sqlNode = result.asStatement
      val sqlText = sqlNode.toSqlString(SparkSqlDialect.DEFAULT).getSql
      logInfo(s"""
          | PPL => SparkSQL
          |   PPL query: $pplText
          |   SQL query: $sqlText
          |""".stripMargin)

      sparkParser.parsePlan(sqlText)
    } catch {
      // Fall back to Spark parse plan logic if flint cannot parse
      case _: ParseException | _: SyntaxCheckException => sparkParser.parsePlan(pplText)
    }
  }

  override def parseExpression(sqlText: String): Expression = sparkParser.parseExpression(sqlText)

  override def parseTableIdentifier(sqlText: String): TableIdentifier =
    sparkParser.parseTableIdentifier(sqlText)

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
    sparkParser.parseFunctionIdentifier(sqlText)

  override def parseMultipartIdentifier(sqlText: String): Seq[String] =
    sparkParser.parseMultipartIdentifier(sqlText)

  override def parseTableSchema(sqlText: String): StructType =
    sparkParser.parseTableSchema(sqlText)

  override def parseDataType(sqlText: String): DataType = sparkParser.parseDataType(sqlText)

  override def parseQuery(sqlText: String): LogicalPlan = sparkParser.parseQuery(sqlText)

  class SparkSchema(spark: SparkSession, database: Database) extends AbstractSchema {

    override def getTableMap: java.util.Map[String, Table] = {
      // TODO: only list tables in the given catalog and database (by parseTableName?)
      spark
        .catalog
        .listTables(database.name)
        .collect()
        .flatMap { table =>
          Some(table.name, new AbstractTable {
            override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
              val builder = typeFactory.builder()
              // TODO: use full table name
              spark.table(table.name).schema.fields.zipWithIndex.foreach { case (f, idx) =>
                val baseType = toCalciteType(f.dataType, typeFactory)
                val nullable = typeFactory.createTypeWithNullability(baseType, f.nullable)
                builder.add(f.name, nullable) // only (String, RelDataType) overload exists
              }
              builder.build()
            }
          }: Table)
        }
        .toMap
        .asJava
    }

    private def toCalciteType(dataType: DataType, typeFactory: RelDataTypeFactory): RelDataType =
      dataType match {

        case IntegerType => typeFactory.createSqlType(SqlTypeName.INTEGER)
        case LongType => typeFactory.createSqlType(SqlTypeName.BIGINT)
        case ShortType => typeFactory.createSqlType(SqlTypeName.SMALLINT)
        case ByteType => typeFactory.createSqlType(SqlTypeName.TINYINT)
        case FloatType => typeFactory.createSqlType(SqlTypeName.FLOAT)
        case DoubleType => typeFactory.createSqlType(SqlTypeName.DOUBLE)
        case BooleanType => typeFactory.createSqlType(SqlTypeName.BOOLEAN)
        case StringType => typeFactory.createSqlType(SqlTypeName.VARCHAR)
        case BinaryType => typeFactory.createSqlType(SqlTypeName.VARBINARY)
        case TimestampType => typeFactory.createSqlType(SqlTypeName.TIMESTAMP)
        case DateType => typeFactory.createSqlType(SqlTypeName.DATE)

        case dt: DecimalType =>
          // DecimalType has precision & scale
          typeFactory.createSqlType(SqlTypeName.DECIMAL, dt.precision, dt.scale)

        case ArrayType(elemType, _) =>
          val elemRel = toCalciteType(elemType, typeFactory)
          typeFactory.createArrayType(elemRel, -1)

        case struct: StructType =>
          // create a Seq[RelDataTypeField], upcasting each Impl → interface
          val fieldDefs: Seq[RelDataTypeField] =
            struct.fields.toSeq.zipWithIndex.map { case (f, idx) =>
              val fldType = toCalciteType(f.dataType, typeFactory)
              // upcast here so the Seq is of the interface type
              new RelDataTypeFieldImpl(f.name, idx, fldType): RelDataTypeField
            }

          // now .asJava yields a java.util.List[RelDataTypeField]
          val fieldsJava: java.util.List[RelDataTypeField] =
            fieldDefs.asJava

          typeFactory.createStructType(fieldsJava)

        case other =>
          throw new UnsupportedOperationException(s"Unsupported Spark type: $other")
      }
  }
}
