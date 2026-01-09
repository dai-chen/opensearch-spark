/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query.calcite

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory, RelDataTypeField}
import org.apache.calcite.sql.`type`.SqlTypeName
import org.opensearch.sql.calcite.`type`.ExprTimeStampType
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory

import org.apache.spark.sql.types._

/**
 * Utility for converting between Spark and Calcite types.
 */
object CalciteTypeConverter {

  /**
   * Convert a Spark DataType to SQL type name string (for unified-query-api compatibility).
   */
  def sparkTypeToSqlTypeName(sparkType: DataType): String = {
    sparkType match {
      case BooleanType => "BOOLEAN"
      case ByteType => "TINYINT"
      case ShortType => "SMALLINT"
      case IntegerType => "INTEGER"
      case LongType => "BIGINT"
      case FloatType => "FLOAT"
      case DoubleType => "DOUBLE"
      case dt: DecimalType => s"DECIMAL(${dt.precision},${dt.scale})"
      case StringType => "VARCHAR"
      case BinaryType => "VARBINARY"
      case DateType => "DATE"
      case TimestampType => "TIMESTAMP"
      case ArrayType(elementType, _) =>
        s"ARRAY<${sparkTypeToSqlTypeName(elementType)}>"
      case MapType(keyType, valueType, _) =>
        s"MAP<${sparkTypeToSqlTypeName(keyType)},${sparkTypeToSqlTypeName(valueType)}>"
      case StructType(fields) =>
        val fieldStrs = fields.map { f =>
          s"${f.name}:${sparkTypeToSqlTypeName(f.dataType)}"
        }
        s"STRUCT<${fieldStrs.mkString(",")}>"
      case _ => "VARCHAR" // Fallback to VARCHAR for unknown types
    }
  }

  /**
   * Convert a Calcite RelDataType to SQL type name string.
   */
  def relDataTypeToSqlTypeName(calciteType: RelDataType): String = {
    calciteType.getSqlTypeName match {
      case SqlTypeName.BOOLEAN => "BOOLEAN"
      case SqlTypeName.TINYINT => "TINYINT"
      case SqlTypeName.SMALLINT => "SMALLINT"
      case SqlTypeName.INTEGER => "INTEGER"
      case SqlTypeName.BIGINT => "BIGINT"
      case SqlTypeName.FLOAT | SqlTypeName.REAL => "FLOAT"
      case SqlTypeName.DOUBLE => "DOUBLE"
      case SqlTypeName.DECIMAL =>
        s"DECIMAL(${calciteType.getPrecision},${calciteType.getScale})"
      case SqlTypeName.CHAR | SqlTypeName.VARCHAR => "VARCHAR"
      case SqlTypeName.BINARY | SqlTypeName.VARBINARY => "VARBINARY"
      case SqlTypeName.DATE => "DATE"
      case SqlTypeName.TIME => "TIME"
      case SqlTypeName.TIMESTAMP | SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE => "TIMESTAMP"
      case SqlTypeName.ARRAY =>
        s"ARRAY<${relDataTypeToSqlTypeName(calciteType.getComponentType)}>"
      case SqlTypeName.MAP =>
        s"MAP<${relDataTypeToSqlTypeName(calciteType.getKeyType)},${relDataTypeToSqlTypeName(calciteType.getValueType)}>"
      case SqlTypeName.ROW =>
        val fields = calciteType.getFieldList
        val fieldStrs = fields.toArray.map { field =>
          val f = field.asInstanceOf[RelDataTypeField]
          s"${f.getName}:${relDataTypeToSqlTypeName(f.getType)}"
        }
        s"STRUCT<${fieldStrs.mkString(",")}>"
      case _ => "VARCHAR" // Fallback to VARCHAR for unknown types
    }
  }

  /**
   * Convert SQL type name string to Spark DataType.
   */
  def sqlTypeNameToSparkType(sqlTypeName: String): DataType = {
    sqlTypeName match {
      case "BOOLEAN" => BooleanType
      case "TINYINT" => ByteType
      case "SMALLINT" => ShortType
      case "INTEGER" => IntegerType
      case "BIGINT" => LongType
      case "FLOAT" => FloatType
      case "DOUBLE" => DoubleType
      case s if s.startsWith("DECIMAL") =>
        // Parse DECIMAL(precision,scale)
        val pattern = """DECIMAL\((\d+),(\d+)\)""".r
        s match {
          case pattern(precision, scale) => DecimalType(precision.toInt, scale.toInt)
          case _ => DecimalType(10, 0) // Default decimal
        }
      case "VARCHAR" | "CHAR" => StringType
      case "VARBINARY" | "BINARY" => BinaryType
      case "DATE" => DateType
      case "TIME" | "TIMESTAMP" => TimestampType
      case s if s.startsWith("ARRAY<") =>
        // Parse ARRAY<elementType>
        val elementTypeName = s.substring(6, s.length - 1)
        ArrayType(sqlTypeNameToSparkType(elementTypeName))
      case s if s.startsWith("MAP<") =>
        // Parse MAP<keyType,valueType>
        val content = s.substring(4, s.length - 1)
        val parts = splitTopLevel(content, ',')
        if (parts.length == 2) {
          MapType(sqlTypeNameToSparkType(parts(0)), sqlTypeNameToSparkType(parts(1)))
        } else {
          MapType(StringType, StringType) // Fallback
        }
      case s if s.startsWith("STRUCT<") =>
        // Parse STRUCT<field1:type1,field2:type2,...>
        val content = s.substring(7, s.length - 1)
        val fieldStrs = splitTopLevel(content, ',')
        val fields = fieldStrs.map { fieldStr =>
          val colonIdx = fieldStr.indexOf(':')
          if (colonIdx > 0) {
            val name = fieldStr.substring(0, colonIdx)
            val typeName = fieldStr.substring(colonIdx + 1)
            StructField(name, sqlTypeNameToSparkType(typeName), nullable = true)
          } else {
            StructField("unknown", StringType, nullable = true)
          }
        }
        StructType(fields)
      case _ => StringType // Fallback to String for unknown types
    }
  }

  /**
   * Helper method to split string at top-level delimiter (ignoring nested brackets).
   */
  private def splitTopLevel(s: String, delimiter: Char): Array[String] = {
    val result = scala.collection.mutable.ArrayBuffer[String]()
    var depth = 0
    var start = 0

    for (i <- 0 until s.length) {
      val c = s.charAt(i)
      if (c == '<' || c == '(') {
        depth += 1
      } else if (c == '>' || c == ')') {
        depth -= 1
      } else if (c == delimiter && depth == 0) {
        result += s.substring(start, i).trim
        start = i + 1
      }
    }

    if (start < s.length) {
      result += s.substring(start).trim
    }

    result.toArray
  }

  /**
   * Convert a Calcite RelDataType to a Spark DataType.
   */
  def toSparkType(calciteType: RelDataType): DataType = {
    calciteType.getSqlTypeName match {
      case SqlTypeName.BOOLEAN => BooleanType
      case SqlTypeName.TINYINT => ByteType
      case SqlTypeName.SMALLINT => ShortType
      case SqlTypeName.INTEGER => IntegerType
      case SqlTypeName.BIGINT => LongType
      case SqlTypeName.FLOAT | SqlTypeName.REAL => FloatType
      case SqlTypeName.DOUBLE => DoubleType
      case SqlTypeName.DECIMAL =>
        DecimalType(calciteType.getPrecision, calciteType.getScale)
      case SqlTypeName.CHAR | SqlTypeName.VARCHAR => StringType
      case SqlTypeName.BINARY | SqlTypeName.VARBINARY => BinaryType
      case SqlTypeName.DATE => DateType
      case SqlTypeName.TIME => TimestampType
      case SqlTypeName.TIMESTAMP | SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE => TimestampType
      case SqlTypeName.ARRAY =>
        ArrayType(toSparkType(calciteType.getComponentType))
      case SqlTypeName.MAP =>
        MapType(toSparkType(calciteType.getKeyType), toSparkType(calciteType.getValueType))
      case SqlTypeName.ROW =>
        val fields = calciteType.getFieldList
        val structFields = fields.toArray.map { field =>
          val f = field.asInstanceOf[RelDataTypeField]
          StructField(f.getName, toSparkType(f.getType), f.getType.isNullable)
        }
        StructType(structFields)
      case _ => StringType // Fallback to String for unknown types
    }
  }

  /**
   * Convert a Spark DataType to a Calcite RelDataType.
   */
  def toCalciteType(sparkType: DataType, typeFactory: RelDataTypeFactory): RelDataType = {
    sparkType match {
      case BooleanType => typeFactory.createSqlType(SqlTypeName.BOOLEAN)
      case ByteType => typeFactory.createSqlType(SqlTypeName.TINYINT)
      case ShortType => typeFactory.createSqlType(SqlTypeName.SMALLINT)
      case IntegerType => typeFactory.createSqlType(SqlTypeName.INTEGER)
      case LongType => typeFactory.createSqlType(SqlTypeName.BIGINT)
      case FloatType => typeFactory.createSqlType(SqlTypeName.FLOAT)
      case DoubleType => typeFactory.createSqlType(SqlTypeName.DOUBLE)
      case dt: DecimalType =>
        typeFactory.createSqlType(SqlTypeName.DECIMAL, dt.precision, dt.scale)
      case StringType => typeFactory.createSqlType(SqlTypeName.VARCHAR)
      case BinaryType => typeFactory.createSqlType(SqlTypeName.VARBINARY)
      case DateType => typeFactory.createSqlType(SqlTypeName.DATE)
      // Convert Spark timestamp type to Calcite UDT
      case TimestampType =>
        new ExprTimeStampType(
          OpenSearchTypeFactory.TYPE_FACTORY
        ) // typeFactory.createSqlType(SqlTypeName.TIMESTAMP)
      case ArrayType(elementType, _) =>
        typeFactory.createArrayType(toCalciteType(elementType, typeFactory), -1)
      case MapType(keyType, valueType, _) =>
        typeFactory.createMapType(
          toCalciteType(keyType, typeFactory),
          toCalciteType(valueType, typeFactory))
      case StructType(fields) =>
        val fieldNames = fields.map(_.name).toList
        val fieldTypes = fields.map(f => toCalciteType(f.dataType, typeFactory)).toList
        typeFactory.createStructType(
          java.util.Arrays.asList(fieldTypes: _*),
          java.util.Arrays.asList(fieldNames: _*))
      case _ => typeFactory.createSqlType(SqlTypeName.ANY)
    }
  }

  /**
   * Convert a Spark value to a format suitable for Calcite execution.
   */
  def sparkToCalciteValue(value: Any, sparkType: DataType): Any = {
    if (value == null) return null

    sparkType match {
      case StringType =>
        value match {
          case utf8: org.apache.spark.unsafe.types.UTF8String => utf8.toString
          case s: String => s
          case _ => value.toString
        }
      case ArrayType(elementType, _) =>
        val array = value.asInstanceOf[org.apache.spark.sql.catalyst.util.ArrayData]
        val converted = new Array[Any](array.numElements())
        var i = 0
        while (i < array.numElements()) {
          converted(i) = sparkToCalciteValue(array.get(i, elementType), elementType)
          i += 1
        }
        java.util.Arrays.asList(converted: _*)
      case StructType(fields) =>
        val row = value.asInstanceOf[org.apache.spark.sql.catalyst.InternalRow]
        val converted = new Array[Any](fields.length)
        var i = 0
        while (i < fields.length) {
          converted(i) = sparkToCalciteValue(row.get(i, fields(i).dataType), fields(i).dataType)
          i += 1
        }
        converted
      case MapType(keyType, valueType, _) =>
        val mapData = value.asInstanceOf[org.apache.spark.sql.catalyst.util.MapData]
        val keys = mapData.keyArray()
        val values = mapData.valueArray()
        val map = new java.util.HashMap[Any, Any]()
        var i = 0
        while (i < keys.numElements()) {
          val key = sparkToCalciteValue(keys.get(i, keyType), keyType)
          val v = sparkToCalciteValue(values.get(i, valueType), valueType)
          map.put(key, v)
          i += 1
        }
        map
      case DateType =>
        // Spark stores dates as days since epoch
        value match {
          case days: Int => java.sql.Date.valueOf(java.time.LocalDate.ofEpochDay(days.toLong))
          case _ => value
        }
      case TimestampType =>
        // Spark stores timestamps as microseconds since epoch
        value match {
          case micros: Long =>
            new java.sql.Timestamp(micros / 1000)
          case _ => value
        }
      case _ => value
    }
  }

  /**
   * Convert a Calcite value back to Spark format.
   */
  def calciteToSparkValue(value: Any, sparkType: DataType): Any = {
    if (value == null) return null

    sparkType match {
      case StringType =>
        value match {
          case s: String => org.apache.spark.unsafe.types.UTF8String.fromString(s)
          case _ => org.apache.spark.unsafe.types.UTF8String.fromString(value.toString)
        }
      case ArrayType(elementType, _) =>
        val list = value.asInstanceOf[java.util.List[_]]
        val converted = new Array[Any](list.size())
        var i = 0
        while (i < list.size()) {
          converted(i) = calciteToSparkValue(list.get(i), elementType)
          i += 1
        }
        org.apache.spark.sql.catalyst.util.ArrayData.toArrayData(converted)
      case DateType =>
        value match {
          case date: java.sql.Date => date.toLocalDate.toEpochDay.toInt
          case _ => value
        }
      case TimestampType =>
        value match {
          case ts: java.sql.Timestamp => ts.getTime * 1000
          case _ => value
        }
      case _ => value
    }
  }
}
