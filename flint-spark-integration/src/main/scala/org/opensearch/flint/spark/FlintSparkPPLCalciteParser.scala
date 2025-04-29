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

import java.util.List

import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.plan.{RelTrait, RelTraitDef}
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider
import org.apache.calcite.rel.rel2sql.RelToSqlConverter
import org.apache.calcite.sql.dialect.SparkSqlDialect
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.tools.{Frameworks, Programs}
import org.opensearch.sql.calcite.{CalcitePlanContext, CalciteRelNodeVisitor}
import org.opensearch.sql.common.antlr.SyntaxCheckException
import org.opensearch.sql.executor.{OpenSearchTypeSystem, QueryType}
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser
import org.opensearch.sql.ppl.parser.AstBuilder

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * Flint PPL parser that parse PPL Query Language into spark logical plan - if parse fails it will
 * fall back to spark's parser.
 *
 * @param sparkParser
 *   Spark SQL parser
 */
class FlintSparkPPLCalciteParser(val spark: SparkSession, sparkParser: ParserInterface)
    extends ParserInterface {

  private val pplParser = new PPLSyntaxParser()

  override def parsePlan(sqlText: String): LogicalPlan = {
    try {
      // Parse to AST
      val cst = pplParser.parse(sqlText)
      val ast = cst.accept(new AstBuilder(sqlText, null))

      // Analyze by Calcite
      val rootSchema = CalciteSchema.createRootSchema(true, false).plus
      val config =
        Frameworks.newConfigBuilder
          .parserConfig(SqlParser.Config.DEFAULT)
          .defaultSchema(rootSchema)
          .traitDefs(null.asInstanceOf[List[RelTraitDef[_ <: RelTrait]]])
          .programs(Programs.calc(DefaultRelMetadataProvider.INSTANCE))
          .typeSystem(OpenSearchTypeSystem.INSTANCE)
          .build()

      val context = CalcitePlanContext.create(config, QueryType.PPL)
      val relNodeVisitor = new CalciteRelNodeVisitor
      val relNode = relNodeVisitor.analyze(ast, context)

      val converter = new RelToSqlConverter(SparkSqlDialect.DEFAULT)
      val result = converter.visitRoot(relNode)
      val sqlNode = result.asStatement
      val sql = sqlNode.toSqlString(SparkSqlDialect.DEFAULT).getSql

      sparkParser.parsePlan(sql)
    } catch {
      // Fall back to Spark parse plan logic if flint cannot parse
      case _: ParseException | _: SyntaxCheckException => sparkParser.parsePlan(sqlText)
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
}
