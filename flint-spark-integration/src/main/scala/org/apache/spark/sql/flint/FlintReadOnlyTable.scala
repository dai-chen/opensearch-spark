/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint

import java.util
import scala.collection.JavaConverters._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql.`type`.SqlTypeName
import org.opensearch.flint.core.storage.OpenSearchClientUtils
import org.opensearch.flint.core.table.OpenSearchCluster
import org.opensearch.sql.calcite.`type`.ExprIPType
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY
import org.opensearch.sql.opensearch.client.OpenSearchRestClient
import org.opensearch.sql.opensearch.storage.OpenSearchStorageEngine
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.catalog.TableCapability.BATCH_READ
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.opensearch.flint.spark.udt.IPAddressUDT

/**
 * FlintReadOnlyTable.
 *
 * @param conf
 *   configuration
 * @param userSpecifiedSchema
 *   userSpecifiedSchema
 */
class FlintReadOnlyTable(
    val conf: util.Map[String, String],
    val userSpecifiedSchema: Option[StructType])
    extends Table
    with SupportsRead {

  lazy val sparkSession = SparkSession.active

  lazy val flintSparkConf: FlintSparkConf = FlintSparkConf(conf)

  lazy val name: String = flintSparkConf.tableName()

  lazy val tables: Seq[org.opensearch.flint.core.Table] =
    OpenSearchCluster.apply(name, flintSparkConf.flintOptions()).asScala

  lazy val resolvedTablesSchema: StructType = tables.headOption
    .map(tbl => { // FlintDataType.deserialize(tbl.schema().asJson())
      // Reuse Calcite's schema instead of Flint's Table for unification
      val osEngine =
        new OpenSearchStorageEngine(
          new OpenSearchRestClient(
            OpenSearchClientUtils.createRestHighLevelClient(FlintSparkConf().flintOptions())),
          null)

      val calciteSchema =
        osEngine
          .getTable(null, name)
          .asInstanceOf[org.apache.calcite.schema.Table]
          .getRowType(TYPE_FACTORY)

      val sparkFields =
        calciteSchema.getFieldList.asScala.map { field =>
          StructField(field.getName, toSparkType(field.getType), field.getType.isNullable)
        }.toArray
      StructType(sparkFields)
    })
    .getOrElse(StructType(Nil))

  lazy val schema: StructType = {
    userSpecifiedSchema.getOrElse { resolvedTablesSchema }
  }

  override def capabilities(): util.Set[TableCapability] =
    util.EnumSet.of(BATCH_READ)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    FlintScanBuilder(tables, schema, flintSparkConf)
  }

  private def toSparkType(calciteType: RelDataType): DataType = {
    if (calciteType.isStruct) {
      // nested struct → recurse
      val nestedFields = calciteType.getFieldList.asScala.map { f =>
        StructField(f.getName, toSparkType(f.getType), f.getType.isNullable)
      }.toArray
      StructType(nestedFields)
    } else {
      // primitive mapping
      calciteType.getSqlTypeName match {
        case SqlTypeName.BOOLEAN => BooleanType
        case SqlTypeName.TINYINT => ByteType
        case SqlTypeName.SMALLINT => ShortType
        case SqlTypeName.INTEGER => IntegerType
        case SqlTypeName.BIGINT => LongType
        case SqlTypeName.FLOAT => FloatType
        case SqlTypeName.DOUBLE => DoubleType
        case SqlTypeName.CHAR | SqlTypeName.VARCHAR => StringType
        case SqlTypeName.DATE => DateType
        case SqlTypeName.TIMESTAMP => TimestampType
        case SqlTypeName.ARRAY =>
          // assume element type at position 0
          val eltType = calciteType.getComponentType
          ArrayType(toSparkType(eltType), containsNull = true)
        case SqlTypeName.MAP =>
          // map<key, value>
          val keyType = calciteType.getKeyType
          val valueType = calciteType.getValueType
          MapType(toSparkType(keyType), toSparkType(valueType), valueContainsNull = true)
          // we didn't register UDT to Calcite Sql type names?
        case SqlTypeName.OTHER => IPAddressUDT
        case _ =>
          // fallback to String
          StringType
      }
    }
  }
}
