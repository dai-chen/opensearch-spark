/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.calcite

import scala.collection.JavaConverters._
import org.apache.calcite.adapter.enumerable._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core._
import org.apache.calcite.rex._
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions => F}
import org.apache.spark.sql.types._
import org.opensearch.sql.opensearch.storage.scan.CalciteEnumerableIndexScan

/**
 * Translates Calcite RelNode physical plans to Spark DataFrame operations
 */
class CalcitePhyPlanToSparkTranslator(spark: SparkSession) {

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

    call.getOperator.getName match {
      // Comparison operators
      case "=" => operandColumns(0) === operandColumns(1)
      case "<>" => operandColumns(0) =!= operandColumns(1)
      case ">" => operandColumns(0) > operandColumns(1)
      case ">=" => operandColumns(0) >= operandColumns(1)
      case "<" => operandColumns(0) < operandColumns(1)
      case "<=" => operandColumns(0) <= operandColumns(1)

      // Logical operators
      case "AND" => operandColumns(0) && operandColumns(1)
      case "OR" => operandColumns(0) || operandColumns(1)
      case "NOT" => !operandColumns(0)

      // Arithmetic operators
      case "+" => operandColumns(0) + operandColumns(1)
      case "-" => operandColumns(0) - operandColumns(1)
      case "*" => operandColumns(0) * operandColumns(1)
      case "/" => operandColumns(0) / operandColumns(1)
      case "MOD" => operandColumns(0) % operandColumns(1)

      // String functions
      case "CONCAT" => F.concat(operandColumns: _*)
      case "UPPER" => F.upper(operandColumns(0))
      case "LOWER" => F.lower(operandColumns(0))
      // case "SUBSTRING" =>
      //  F.substring(operandColumns(0), operandColumns(1), operandColumns(2))

      // Type conversions
      case "CAST" =>
        val targetType = convertCalciteTypeToSparkType(call.getType)
        operandColumns(0).cast(targetType)

      // Other operators
      case "CASE" =>
        // CASE WHEN ... THEN ... ELSE ... END
        val numOperands = operandColumns.size
        val elseColumn = operandColumns.last

        // Build the CASE expression
        var caseExpr = elseColumn
        for (i <- (0 until (numOperands - 1) / 2).reverse) {
          val whenColumn = operandColumns(i * 2)
          val thenColumn = operandColumns(i * 2 + 1)
          caseExpr = F.when(whenColumn, thenColumn).otherwise(caseExpr)
        }
        caseExpr

      case "IS NULL" => operandColumns(0).isNull
      case "IS NOT NULL" => operandColumns(0).isNotNull

      // case "LIKE" => operandColumns(0).like(operandColumns(1))

      // Handle other functions - could add many more
      case name =>
        throw new UnsupportedOperationException(s"Unsupported operator: $name")
    }
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
