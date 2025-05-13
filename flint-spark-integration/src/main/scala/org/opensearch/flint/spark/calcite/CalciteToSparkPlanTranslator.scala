/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.calcite

import java.util.Locale
import scala.collection.JavaConverters._
import org.apache.calcite.DataContext
import org.apache.calcite.adapter.enumerable._
import org.apache.calcite.interpreter.{Context, JaninoRexCompiler}
import org.apache.calcite.jdbc.JavaTypeFactoryImpl
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core._
import org.apache.calcite.rex._
import org.apache.calcite.sql.`type`.SqlTypeName
import org.opensearch.sql.opensearch.storage.scan.CalciteEnumerableIndexScan
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession, functions => F}
import org.apache.spark.sql.api.java.{UDF0, UDF1, UDF2, UDF3}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.opensearch.flint.spark.udt.IPAddressUDT
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY
import org.opensearch.sql.calcite.`type`.ExprIPType

/**
 * Translates Calcite RelNode physical plans to Spark DataFrame operations
 */
class CalciteToSparkPlanTranslator(spark: SparkSession) {

  /**
   * Main entry point: Convert a Calcite RelNode tree to a Spark DataFrame
   */
  def translate(node: RelNode): DataFrame = {
    node match {
      case project: EnumerableProject =>
        translateProject(project)

      case filter: EnumerableFilter =>
        translateFilter(filter)

      // case aggregate: EnumerableAggregate =>
      //  translateAggregate(aggregate)

      case sort: EnumerableSort =>
        translateSort(sort)

      case join: Join =>
        translateJoin(join)

      case tableScan: EnumerableTableScan =>
        translateTableScan(tableScan)

      case indexScan: CalciteEnumerableIndexScan =>
        translateIndexScan(indexScan)

      case values: EnumerableValues =>
        translateValues(values)

      case _ =>
        throw new UnsupportedOperationException(
          s"Unsupported RelNode type: ${node.getClass.getName}")
    }
  }

  /**
   * Translate Project to DataFrame select()
   */
  private def translateProject(project: EnumerableProject): DataFrame = {
    val input = translate(project.getInput)

    // Translate each projection expression to a Column
    val columns = project.getProjects.asScala.zipWithIndex.map { case (rexNode, i) =>
      val outputName = project.getRowType.getFieldNames.get(i)
      val column = translateRexNodeToColumn(rexNode, input)
      column.as(outputName)
    }

    input.select(columns: _*)
  }

  /**
   * Translate Filter to DataFrame filter()
   */
  private def translateFilter(filter: EnumerableFilter): DataFrame = {
    val input = translate(filter.getInput)
    val condition = translateRexNodeToColumn(filter.getCondition, input)
    input.filter(condition)
  }

  /**
   * Translate Aggregate to DataFrame groupBy().agg()
   */
  /*
  private def translateAggregate(agg: EnumerableAggregate): DataFrame = {
    val input = translate(agg.getInput)

    // Group by columns
    val groupingColumns = agg.getGroupSet.asScala.map { idx =>
      input.columns(idx)
    }

    // If no grouping columns, use standard agg() without groupBy
    if (groupingColumns.isEmpty) {
      // Translate aggregate expressions
      val aggColumns = agg.getAggCallList.asScala.map { aggCall =>
        val aggFunction = createAggregateFunction(aggCall, input)
        val aggName =
          if (aggCall.getName != null) aggCall.getName
          else s"${aggCall.getAggregation.getName}_${aggCall.getArgList}"
        aggFunction.as(aggName)
      }

      input.agg(aggColumns.head, aggColumns.tail: _*)
    } else {
      // Create groupBy expression
      val grouped = input.groupBy(groupingColumns.map(col => F.col(col)): _*)

      // Translate aggregate expressions
      val aggColumns = agg.getAggCallList.asScala.map { aggCall =>
        val aggFunction = createAggregateFunction(aggCall, input)
        val aggName =
          if (aggCall.getName != null) aggCall.getName
          else s"${aggCall.getAggregation.getName}_${aggCall.getArgList}"
        aggFunction.as(aggName)
      }

      // Apply aggregations
      if (aggColumns.isEmpty) {
        // Only grouping, no aggregates
        grouped.count().drop("count")
      } else {
        grouped.agg(aggColumns.head, aggColumns.tail: _*)
      }
    }
  }
   */

  /**
   * Translate Sort to DataFrame orderBy()
   */
  private def translateSort(sort: EnumerableSort): DataFrame = {
    val input = translate(sort.getInput)

    // Convert sort fields to Column expressions with direction
    val sortColumns = sort.getCollation.getFieldCollations.asScala.map { collation =>
      val fieldName = input.columns(collation.getFieldIndex)
      val col = F.col(fieldName)

      if (collation.getDirection.isDescending) {
        col.desc
      } else {
        col.asc
      }
    }

    input.orderBy(sortColumns: _*)
  }

  /**
   * Translate Join to DataFrame join()
   */
  private def translateJoin(join: Join): DataFrame = {
    val left = translate(join.getLeft)
    val right = translate(join.getRight)

    // Translate join condition
    val condition = if (join.getCondition != null) {
      translateRexNodeToColumn(join.getCondition, left, right)
    } else {
      null // Natural join or cross join
    }

    // Map join type to Spark join type
    val joinType = join.getJoinType match {
      case JoinRelType.INNER => "inner"
      case JoinRelType.LEFT => "left_outer"
      case JoinRelType.RIGHT => "right_outer"
      case JoinRelType.FULL => "full_outer"
      case JoinRelType.SEMI => "left_semi"
      case JoinRelType.ANTI => "left_anti"
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported join type: ${join.getJoinType}")
    }

    left.join(right, condition, joinType)
  }

  /**
   * Translate TableScan to DataFrame
   */
  private def translateTableScan(scan: EnumerableTableScan): DataFrame = {
    val tableName = scan.getTable.getQualifiedName.asScala

    // Create a DataFrame from the table
    if (tableName.size == 2) {
      // Database.table format
      spark.table(s"${tableName(0)}.${tableName(1)}")
    } else if (tableName.size >= 3) {
      // Catalog.database.table format
      spark.table(s"${tableName(0)}.${tableName(1)}.${tableName(2)}")
    } else {
      // Just table name
      spark.table(tableName(0))
    }
  }

  private def translateIndexScan(scan: CalciteEnumerableIndexScan): DataFrame = {
    val tableName = scan.getTable.getQualifiedName.asScala

    // Create a DataFrame from the table
    if (tableName.size == 2) {
      // Database.table format
      spark.table(s"${tableName(0)}.${tableName(1)}")
    } else if (tableName.size >= 3) {
      // Catalog.database.table format
      spark.table(s"${tableName(0)}.${tableName(1)}.${tableName(2)}")
    } else {
      // Just table name
      spark.table(tableName(0))
    }
  }

  /**
   * Translate Values to DataFrame
   */
  private def translateValues(values: EnumerableValues): DataFrame = {
    // Convert tuples to rows
    val rows = values.getTuples.asScala.map { tuple =>
      org.apache.spark.sql.Row.fromSeq(tuple.asScala.map(rexNode => {
        // Evaluate literals to their Java values
        rexNode match {
          case lit: RexLiteral => lit.getValue3
          case _ => null // Should only contain literals
        }
      }))
    }

    // Create schema from field types
    val schema = StructType(values.getRowType.getFieldList.asScala.map { field =>
      StructField(field.getName, convertCalciteTypeToSparkType(field.getType), nullable = true)
    })

    // Create DataFrame from rows and schema
    spark.createDataFrame(rows.asJava, schema)
  }

  /**
   * Create an aggregate function Column expression
   */
  private def createAggregateFunction(aggCall: AggregateCall, input: DataFrame): Column = {
    // Convert argument list to columns
    val argColumns = aggCall.getArgList.asScala.map { argIdx =>
      F.col(input.columns(argIdx.intValue()))
    }

    // Map Calcite's aggregate function to Spark's
    aggCall.getAggregation.getName match {
      case "COUNT" if aggCall.getArgList.isEmpty =>
        F.count(F.lit(1)) // COUNT(*)
      case "COUNT" => F.count(argColumns.head)
      case "SUM" => F.sum(argColumns.head)
      case "MIN" => F.min(argColumns.head)
      case "MAX" => F.max(argColumns.head)
      case "AVG" => F.avg(argColumns.head)
      case name =>
        throw new UnsupportedOperationException(s"Unsupported aggregation: $name")
    }
  }

  /**
   * Translate a RexNode to a Spark Column
   */
  private def translateRexNodeToColumn(rexNode: RexNode, inputs: DataFrame*): Column = {
    rexNode match {
      case inputRef: RexInputRef =>
        // Find which input DataFrame this reference belongs to
        var remainingIdx = inputRef.getIndex
        for (input <- inputs) {
          if (remainingIdx < input.columns.length) {
            return input.col(input.columns(remainingIdx))
          }
          remainingIdx -= input.columns.length
        }
        throw new IllegalArgumentException(s"Invalid input reference index: ${inputRef.getIndex}")

      case literal: RexLiteral =>
        // Convert Calcite literals to Spark literals
        if (literal.getValue == null) {
          F.lit(null)
        } else {
          literal.getType.getSqlTypeName match {
            case SqlTypeName.CHAR | SqlTypeName.VARCHAR =>
              F.lit(literal.getValue3.toString)
            case SqlTypeName.BOOLEAN =>
              F.lit(literal.getValue3.asInstanceOf[Boolean])
            case SqlTypeName.DECIMAL =>
              F.lit(literal.getValue3.asInstanceOf[java.math.BigDecimal])
            case SqlTypeName.INTEGER =>
              F.lit(literal.getValue3.asInstanceOf[Number].intValue())
            case SqlTypeName.BIGINT =>
              F.lit(literal.getValue3.asInstanceOf[Number].longValue())
            case SqlTypeName.DOUBLE =>
              F.lit(literal.getValue3.asInstanceOf[Number].doubleValue())
            case SqlTypeName.FLOAT =>
              F.lit(literal.getValue3.asInstanceOf[Number].floatValue())
            case SqlTypeName.DATE =>
              F.lit(java.sql.Date.valueOf(literal.getValue3.toString))
            case SqlTypeName.TIMESTAMP =>
              F.lit(java.sql.Timestamp.valueOf(literal.getValue3.toString))
            case _ => F.lit(literal.getValue3)
          }
        }

      case call: RexCall =>
        // Convert function calls
        translateRexCallToColumn(call, inputs: _*)

      case _ =>
        throw new UnsupportedOperationException(
          s"Unsupported RexNode type: ${rexNode.getClass.getName}")
    }
  }

  /**
   * Translate a RexCall (function call) to a Spark Column
   */
  private def translateRexCallToColumn(call: RexCall, inputs: DataFrame*): Column = {
    // Translate the operands first
    val operandColumns =
      call.getOperands.asScala.map(node => translateRexNodeToColumn(node, inputs: _*))

    // Handle common operators directly for better performance
    call.getOperator.getName match {
      // Comparison operators
      case "=" => return operandColumns(0) === operandColumns(1)
      case "<>" => return operandColumns(0) =!= operandColumns(1)
      case ">" => return operandColumns(0) > operandColumns(1)
      case ">=" => return operandColumns(0) >= operandColumns(1)
      case "<" => return operandColumns(0) < operandColumns(1)
      case "<=" => return operandColumns(0) <= operandColumns(1)

      // Logical operators
      case "AND" => return operandColumns(0) && operandColumns(1)
      case "OR" => return operandColumns(0) || operandColumns(1)
      case "NOT" => return !operandColumns(0)

      // Arithmetic operators
      case "+" => return operandColumns(0) + operandColumns(1)
      case "-" => return operandColumns(0) - operandColumns(1)
      case "*" => return operandColumns(0) * operandColumns(1)
      case "/" => return operandColumns(0) / operandColumns(1)
      case "MOD" => return operandColumns(0) % operandColumns(1)

      // A few other common ones
      case "CONCAT" => return F.concat(operandColumns: _*)
      case "UPPER" => return F.upper(operandColumns(0))
      case "LOWER" => return F.lower(operandColumns(0))
      case "IS NULL" => return operandColumns(0).isNull
      case "IS NOT NULL" => return operandColumns(0).isNotNull
      case "CAST" =>
        val targetType = convertCalciteTypeToSparkType(call.getType)
        return operandColumns(0).cast(targetType)

      // For other operators - use JaninoRexCompiler approach
      case _ => // Continue to UDF generation below
    }

    // If we reach here, we need to generate a UDF for this function

    // 1) Build a Calcite row type from input schemas
    val typeFactory = TYPE_FACTORY
    val fields = inputs.toList.flatMap(_.schema.fields)
    val sqlTypes: java.util.List[RelDataType] = fields
      .map(f => typeFactory.createSqlType(
        f.dataType match {
          case StringType => SqlTypeName.VARCHAR
          case LongType => SqlTypeName.BIGINT
          case IPAddressUDT => new ExprIPType(typeFactory).getSqlTypeName
          case _ =>
            SqlTypeName.valueOf(f.dataType.typeName.toUpperCase(Locale.ROOT))
        }
      )).asJava
    val fieldNames: java.util.List[String] = fields.map(_.name).asJava
    val inputRowType = typeFactory.createStructType(sqlTypes, fieldNames)

    // 2) Create RexBuilder and JaninoRexCompiler
    val rexBuilder = new RexBuilder(typeFactory)
    val compiler = new JaninoRexCompiler(rexBuilder)

    // 3) Compile the expression - this returns a Scalar.Producer
    val scalarProducer = compiler.compile(java.util.Arrays.asList(call), inputRowType)

    // Create a DataContext (can be null for most cases if your function doesn't use it)
    val dataContext = new DataContext {
      override def getRootSchema = null
      override def getTypeFactory = typeFactory
      override def getQueryProvider = null
      override def get(name: String) = null
    }

    // Get the scalar from the producer
    val scalar = scalarProducer.apply(dataContext)

    // 4) Generate a unique UDF name
    val udfName = s"calcite_udf_${System.currentTimeMillis}_${System.nanoTime % 10000}"
    val sparkReturnType = convertCalciteTypeToSparkType(call.getType)

    // 5) Determine the arity of the function based on max input ref
    // val maxInputRef = findMaxInputRef(call)
    val inputCount = call.getOperands.size // maxInputRef + 1

    // 6) Create an instance of Context outside UDF for accessibility
    // Keep a reference to the scalar for UDF execution
    val scalarRef = scalar

    // 7) Register UDF with appropriate arity
    inputCount match {
      case 0 =>
        // Nullary function
        val func = new UDF0[Any] {
          override def call(): Any = {
            // Create context inside call method
            val values = Array.empty[AnyRef]
            scalarRef.execute(null, values)
          }
        }
        spark.udf.register(udfName, func, sparkReturnType)

      case 1 =>
        // Unary function
        val func = new UDF1[Any, Any] {
          override def call(a: Any): Any = {
            val values = Array(a.asInstanceOf[AnyRef])
            scalarRef.execute(null, values)
          }
        }
        spark.udf.register(udfName, func, sparkReturnType)

      case 2 =>
        // Binary function
        val func = new UDF2[Any, Any, Any] {
          override def call(a: Any, b: Any): Any = {
            val values = Array(a.asInstanceOf[AnyRef], b.asInstanceOf[AnyRef])
            scalarRef.execute(null, values)
          }
        }
        spark.udf.register(udfName, func, sparkReturnType)

      case 3 =>
        // Ternary function
        /*
        val func = new UDF3[Any, Any, Any, Any] {
          override def call(a: Any, b: Any, c: Any): Any = {
            val values = Array(
              a.asInstanceOf[AnyRef],
              b.asInstanceOf[AnyRef],
              c.asInstanceOf[AnyRef])
            scalarRef.execute(null, values)
          }
        }
        spark.udf.register(udfName, func, sparkReturnType)
         */

        val udfCompiler = new JaninoSparkUdfCompiler(rexBuilder)
        spark.udf.register(udfName, udfCompiler.compile(java.util.Arrays.asList(call), inputRowType), sparkReturnType)

      case _ =>
        // For more than 3 arguments, we need a different approach
        throw new UnsupportedOperationException("Not yet implemented")
    }

    // 8) Call the UDF with the operand columns
    F.callUDF(udfName, operandColumns: _*)
  }

  /**
   * Convert Calcite RelDataType to Spark DataType
   */
  private def convertCalciteTypeToSparkType(relDataType: RelDataType): DataType = {
    relDataType.getSqlTypeName match {
      case SqlTypeName.VARCHAR | SqlTypeName.CHAR => StringType
      case SqlTypeName.BOOLEAN => BooleanType
      case SqlTypeName.INTEGER => IntegerType
      case SqlTypeName.SMALLINT => ShortType
      case SqlTypeName.TINYINT => ByteType
      case SqlTypeName.BIGINT => LongType
      case SqlTypeName.DECIMAL =>
        DecimalType(math.min(38, relDataType.getPrecision), math.min(38, relDataType.getScale))
      case SqlTypeName.FLOAT => FloatType
      case SqlTypeName.DOUBLE => DoubleType
      case SqlTypeName.DATE => DateType
      case SqlTypeName.TIMESTAMP => TimestampType

      case SqlTypeName.ARRAY =>
        val elementType = convertCalciteTypeToSparkType(relDataType.getComponentType)
        ArrayType(elementType)

      case SqlTypeName.MAP =>
        val keyType = convertCalciteTypeToSparkType(relDataType.getKeyType)
        val valueType = convertCalciteTypeToSparkType(relDataType.getValueType)
        MapType(keyType, valueType)

      case SqlTypeName.STRUCTURED =>
        val fields = relDataType.getFieldList.asScala.map { field =>
          StructField(
            field.getName,
            convertCalciteTypeToSparkType(field.getType),
            field.getType.isNullable)
        }
        StructType(fields)

      case _ => StringType // Default fallback
    }
  }
}
