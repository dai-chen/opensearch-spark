/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query.catalog

import java.util

import scala.collection.JavaConverters._

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory, RelDataTypeField, RelDataTypeFieldImpl}
import org.apache.calcite.schema.{Schema, Table}
import org.apache.calcite.schema.impl.{AbstractSchema, AbstractTable}
import org.apache.calcite.sql.`type`.SqlTypeName

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/**
 * Implements Calcite's Schema interface by bridging Spark SQL catalogs and tables.
 *
 * @param catalogName
 *   The name of the catalog this schema represents
 * @param spark
 *   The current SparkSession
 */
class SparkSchema(spark: SparkSession, catalogName: String) extends AbstractSchema {

  override protected def getSubSchemaMap: util.Map[String, Schema] =
    new LazyMap[String, Schema](dbName => buildSubSchema(dbName))

  private def buildSubSchema(dbName: String): Schema = new AbstractSchema() {
    override def getTableMap: util.Map[String, Table] =
      new LazyMap[String, Table](tableName => buildTable(dbName, tableName))
  }

  private def buildTable(dbName: String, tableName: String): Table =
    new AbstractTable {
      override def getRowType(factory: RelDataTypeFactory): RelDataType = {
        val builder = factory.builder()
        val table = spark.table(s"$catalogName.$dbName.$tableName")
        table.schema.fields.foreach { field =>
          val baseType = toCalciteType(field.dataType, factory)
          val nullableType = factory.createTypeWithNullability(baseType, field.nullable)
          builder.add(field.name, nullableType)
        }
        builder.build()
      }
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

  /**
   * A read-only map that computes values on demand using the provided function.
   *
   * @param valueFn
   *   function that computes the value for a given key
   * @tparam K
   *   key type
   * @tparam V
   *   value type
   */
  private class LazyMap[K, V](valueFn: K => V) extends util.HashMap[K, V] {
    override def get(key: Any): V = valueFn(key.asInstanceOf[K])
  }
}
