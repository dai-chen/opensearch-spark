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

import java.util
import java.util.{Collections, List}
import scala.collection.JavaConverters._
import org.apache.calcite.adapter.enumerable.{EnumerableConvention, EnumerableProject, EnumerableRel, EnumerableRules, EnumerableTableScan, RexToLixTranslator}
import org.apache.calcite.interpreter.Bindables
import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.plan.hep.{HepPlanner, HepProgramBuilder}
import org.apache.calcite.plan.{RelOptTable, RelTrait, RelTraitDef}
import org.apache.calcite.rel.{RelHomogeneousShuttle, RelNode}
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory, RelDataTypeField, RelDataTypeFieldImpl}
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.logical.LogicalTableScan
import org.apache.calcite.rel.rel2sql.RelToSqlConverter
import org.apache.calcite.schema.{Table, TranslatableTable}
import org.apache.calcite.schema.impl.{AbstractSchema, AbstractTable}
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.dialect.SparkSqlDialect
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.tools.{Frameworks, Programs}
import org.opensearch.flint.core.storage.OpenSearchClientUtils
import org.opensearch.flint.spark.calcite.{CalciteToSparkPlanTranslator, CustomSparkSqlDialect, OpenSearchIndexScanToTableFunctionRule}
import org.opensearch.sql.ast.expression.QualifiedName
import org.opensearch.sql.ast.statement.Query
import org.opensearch.sql.calcite.{CalcitePlanContext, CalciteRelNodeVisitor}
import org.opensearch.sql.common.antlr.SyntaxCheckException
import org.opensearch.sql.executor.{OpenSearchTypeSystem, QueryType}
import org.opensearch.sql.opensearch.client.OpenSearchRestClient
import org.opensearch.sql.opensearch.storage.OpenSearchStorageEngine
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser
import org.opensearch.sql.ppl.parser.{AstBuilder, AstStatementBuilder}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.types._

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

  private val pplParser = new PPLSyntaxParser()

  override def parsePlan(pplText: String): LogicalPlan = {
    try {
      // Parse to AST
      val cst = pplParser.parse(pplText)
      val statement =
        cst.accept(
          new AstStatementBuilder(
            new AstBuilder(pplText),
            AstStatementBuilder.StatementBuilderContext.builder
              .isExplain(false)
              .format("jdbc")
              .build))
      val ast = statement.asInstanceOf[Query].getPlan

      // Register each Spark catalog to Calcite schema
      val rootSchema = CalciteSchema.createRootSchema(true, false).plus() // SchemaPlus
      /*
      val catalogManager = spark.sessionState.catalogManager
      catalogManager.listCatalogs(Option.empty).foreach { catalog =>
        logInfo(s"Registering Spark catalog $catalog to Calcite")
        val sparkCatalog = rootSchema.add(catalog, new AbstractSchema())
        val calciteSchema =
          catalog match {
            case "dev" => new OpenSearchSchema // OS catalog name in IT
            case _ => new SparkSchema(spark)
          }
        sparkCatalog.add("default", calciteSchema)
      }
       */
      rootSchema
        .add("dev", new AbstractSchema())
        .add("default", new OpenSearchSchema)
      rootSchema
        .add("spark_catalog", new AbstractSchema())
        .add("default", new SparkSchema(spark))

      // Analyze by Calcite
      val config =
        Frameworks.newConfigBuilder
          .parserConfig(SqlParser.Config.DEFAULT)
          .defaultSchema(rootSchema)
          .traitDefs(null.asInstanceOf[List[RelTraitDef[_ <: RelTrait]]])
          // The program below causes stackoverflow when physical planning
          // .programs(Programs.calc(DefaultRelMetadataProvider.INSTANCE))
          .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, false, 2))
          .typeSystem(OpenSearchTypeSystem.INSTANCE)
          .build()

      val context = CalcitePlanContext.create(config, QueryType.PPL)
      val relNodeVisitor = new CalciteRelNodeVisitor
      val relNode = relNodeVisitor.analyze(ast, context)

      val converter = new RelToSqlConverter(SparkSqlDialect.DEFAULT)
      val result = converter.visitRoot(relNode)
      val sqlNode = result.asStatement
      val sqlText = sqlNode.toSqlString(SparkSqlDialect.DEFAULT).getSql
      logInfo(s"""
          | PPL => SparkSQL
          |   PPL query: $pplText
          |   SQL query: $sqlText
          |""".stripMargin)

      // Optional: generate logical optimized plan
      val shuttle = new RelHomogeneousShuttle() {
        override def visit(scan: TableScan): RelNode = {
          val table = scan.getTable
          if (scan.isInstanceOf[LogicalTableScan] && Bindables.BindableTableScan.canHandle(
              table)) {
            // Always replace the LogicalTableScan with BindableTableScan
            // because it's implementation does not require a "schema" as context.
            return Bindables.BindableTableScan.create(scan.getCluster, table)
          }
          super.visit(scan)
        }
      }
      val rel2 = relNode.accept(shuttle)
      logInfo(s"Calcite physical plan 1: $rel2")

      // Generate physical plan
      val ruleProgram = config.getPrograms.get(0)
      val planner = relNode.getCluster.getPlanner
      val traitSet = relNode.getTraitSet.replace(EnumerableConvention.INSTANCE)
      // run the optimizer (this is where EnumerableIndexScanRule & your OpenSearchIndexRules get registered)
      val optimizedRel =
        ruleProgram.run(
          planner,
          relNode,
          traitSet,
          Collections.emptyList(),
          Collections.emptyList())
      logInfo(s"Calcite physical plan 2: $optimizedRel")
      logInfo(s"SparkSQL query for physical plan: ${calcitePlanToSparkSql(optimizedRel)}")

      val sparkDf = new CalciteToSparkPlanTranslator(spark).translate(optimizedRel)
      logInfo(s"Spark plan translated from Calcite plan:")
      sparkDf.explain(true)
      sparkDf.show

      // Post processing before toSparkSql
      // Create a HepPlanner with just our rule
      val program = new HepProgramBuilder()
        .addRuleInstance(new OpenSearchIndexScanToTableFunctionRule())
        .build()

      val planner2 = new HepPlanner(program)
      planner2.setRoot(optimizedRel)
      val calcitePlan2 = planner2.findBestExp()
      val sparkSqlFromCalcitePhy = calcitePlanToSparkSql(calcitePlan2)

      logInfo(s"SparkSQL query from Calcite physical plan: $sparkSqlFromCalcitePhy")
      spark.sql(sparkSqlFromCalcitePhy).show

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

  class OpenSearchSchema extends AbstractSchema {
    private val osEngine =
      new OpenSearchStorageEngine(
        new OpenSearchRestClient(
          OpenSearchClientUtils.createRestHighLevelClient(FlintSparkConf().flintOptions())),
        null)

    private val tableMap: util.Map[String, Table] =
      new util.HashMap[String, Table]() {
        override def get(key: AnyRef): Table = {
          if (!super.containsKey(key)) {
            val fullName = new QualifiedName(key.asInstanceOf[String])
            osEngine
              .getTable(null, fullName.getSuffix)
              .asInstanceOf[Table]
          } else {
            super.get(key)
          }
        }
      }

    override def getTableMap: util.Map[String, Table] = tableMap
  }

  class SparkSchema(spark: SparkSession) extends AbstractSchema {

    override def getTableMap: java.util.Map[String, Table] = {
      // 1) build a Scala Map[String,Table]
      val scalaMap: Map[String, Table] = spark.catalog
        .listTables()
        .collect()
        .map { t =>
          // qualify with database if needed
          val fullName =
            if (t.database.isEmpty || t.database == spark.catalog.currentDatabase) t.name
            else s"${t.database}.${t.name}"

          // upcast to Table here:
          val calciteTable: Table = new AbstractTable with TranslatableTable {
            override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
              val builder = typeFactory.builder()
              val df = spark.table(fullName)

              // for each Spark field, create a Calcite type + apply nullability
              df.schema.fields.zipWithIndex.foreach { case (f, idx) =>
                val baseType = toCalciteType(f.dataType, typeFactory)
                val nullable = typeFactory.createTypeWithNullability(baseType, f.nullable)
                builder.add(f.name, nullable) // only (String, RelDataType) overload exists
              }
              builder.build()
            }

            override def toRel(
                context: RelOptTable.ToRelContext,
                relOptTable: RelOptTable): RelNode = {
              // Create an EnumerableTableScan which is a physical node with ENUMERABLE convention
              val cluster = context.getCluster
              val traitSet = cluster.traitSet.replace(EnumerableConvention.INSTANCE)
              new EnumerableTableScan(
                cluster, // cluster
                traitSet, // trait set with ENUMERABLE convention
                relOptTable, // table
                null // elementType can be null
              )
            }
          }

          fullName -> calciteTable
        }
        .toMap // Map[String,Table]

      // 2) convert to Java Map[String,Table]
      scalaMap.asJava
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

  private def calcitePlanToSparkSql(plan: RelNode): String = {
    val dialect = new CustomSparkSqlDialect()
    val converter = new RelToSqlConverter(dialect)
    val result = converter.visitRoot(plan)
    val sqlNode = result.asStatement
    sqlNode.toSqlString(dialect).getSql
  }
}
