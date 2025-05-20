/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.calcite.sql.validate.SqlUserDefinedFunction
import org.opensearch.flint.spark.FlintSparkSuite
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.opensearch.sql.data.model.ExprValueUtils
import org.opensearch.sql.expression.datetime.DateTimeFunctions
import org.opensearch.sql.expression.function.PPLBuiltinOperators

class FlintSparkPPLCalciteITSuite extends FlintSparkSuite {

  private val osCatalogName = "dev"

  /** Test table and index name */
  private val testTable = "spark_catalog.default.flint_ppl_test"

  override def sparkConf: SparkConf = {
    super.sparkConf
      // Register your dev catalog
      .set(
        s"spark.sql.catalog.$osCatalogName",
        "org.apache.spark.opensearch.catalog.OpenSearchCatalog")
      // connection settings under the same prefix
      .set(s"spark.sql.catalog.$osCatalogName.opensearch.host", openSearchHost)
      .set(s"spark.sql.catalog.$osCatalogName.opensearch.port", openSearchPort.toString)
      .set(s"spark.sql.catalog.$osCatalogName.opensearch.write.refresh_policy", "wait_for")
      .set("spark.sql.session.timeZone", "UTC")
      // .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // .set("spark.kryo.registrationRequired", "false")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Create test table
    createPartitionedStateCountryTable(testTable)
  }

  test("Calcite-PPL basic query with OS index") {
    val indexName = "t0001"
    withIndexName(indexName) {
      simpleIndex(indexName)
      val df = spark.sql(
        s"source = $osCatalogName.default.$indexName | where eventName = 'event' | eval accountIdSub = substring(accountId, 2) | fields accountIdSub")
      df.explain(true)
      df.show
    }
  }

  test("Calcite-PPL basic query with OS index meta fields") {
    val indexName = "t0001"
    withIndexName(indexName) {
      val mappings = """{
                       |  "properties": {
                       |    "my_ip": {
                       |      "type": "ip"
                       |    },
                       |    "alias": {
                       |      "type": "alias",
                       |      "path": "my_ip"
                       |    }
                       |  }
                       |}""".stripMargin
      val docs = Seq("""{"my_ip": "192.168.0.1"}""", """{"my_ip": "127.0.0.1"}""")
      index(indexName, oneNodeSetting, mappings, docs)

      val df = spark.sql(s"source = $osCatalogName.default.$indexName | fields _id, my_ip, alias")
      df.explain(true)
      df.printSchema()
      df.show
    }
  }

  test("Calcite-PPL basic query with OS index join S3") {
    val indexName = "http_logs"
    withIndexName(indexName) {
      val mappings = """{
                       |  "properties": {
                       |    "id": {
                       |      "type": "integer"
                       |    },
                       |    "clientip": {
                       |      "type": "ip"
                       |    },
                       |    "status": {
                       |      "type": "integer"
                       |    }
                       |  }
                       |}""".stripMargin
      val docs = Seq(
        """{"id": 1, "clientip": "192.168.0.1", "status": 404}""",
        """{"id": 2, "clientip": "127.0.0.1", "status": 200}""",
        """{"id": 3, "clientip": "198.168.0.100", "status": 200}""")
      index(indexName, oneNodeSetting, mappings, docs)

      val tableName = "ip_table"
      withTable(tableName) {
        createIpAddressTable(tableName)

        val df = spark.sql(s"""
           | source = $osCatalogName.default.$indexName |
           | where status = 200 |
           | lookup spark_catalog.default.$tableName id |
           | eval isValidV6 = if(true, isV6, isValid), description = substring(ipAddress, 3, 3) |
           | fields clientip, isValidV6, description
           |""".stripMargin)
        df.explain(true)
        df.show
      }
    }
  }

  test(s"Calcite-PPL basic query") {
    val df = sql(s"source = $testTable | eval f = crc32(name) | fields f")
    df.explain(true)
    df.show
  }

  test(s"Calcite-PPL basic query with UDF") {
    // Issue 1: hard to infer function signature and register by generic code
    // Issue 2: UDF performance penalty
    /*
    spark.udf.register(
      "get_format", // SQL name
      (t: String, s: String) => { // Spark wrapper
        // delegate straight back into your PPL function
        val fmtFn = new GetFormatFunction()
        fmtFn.eval(t, s).asInstanceOf[String]
      },
      StringType // Spark return type
    )
    */

    spark.udf.register(
      "get_format",
      (`type`: String, format: String) => {
        DateTimeFunctions.exprGetFormat(
          ExprValueUtils.fromObjectValue(`type`),
          ExprValueUtils.fromObjectValue(format)
        ).valueForCalcite()
      },
      StringType
    )
    val df = sql(s"source = $testTable | eval f = GET_FORMAT(DATE, 'USA') | fields f")
    df.explain(true)
    df.show

    // Not sure how because each operator only has Implementor (lambda) for generating Linq expression at query time
    val pplOperators = new PPLBuiltinOperators
    val pplOperatorTable = pplOperators.init()
    pplOperatorTable.getOperatorList.forEach { case pplOperator: SqlUserDefinedFunction =>
      logError(
        s"""
           | PPL function: $pplOperator
           | - name: ${pplOperator.getName}
           | - params: ${pplOperator.getFunction.getParameters}
           |""".stripMargin)
    }
  }

  private def registerGetFormat(): Unit = {


  }

  test(s"Calcite-PPL basic query with overridden UDF") {
    spark.udf.register(
      "char_length", // length is translated to char_length by SparkSQL dialect
      (s: String) => "abc",
      StringType)

    val query = s"source = $testTable | eval f = LENGTH(name) | fields f"
    val df = sql(query)
    df.explain(true)
    df.show
  }

  test(s"Calcite-PPL basic query with nested built-in function") {
    val query =
      s"source = $testTable | eval f = SUBSTRING(SUBSTRING(name, 0 + 1), 1 + 1) | fields f"
    val df = sql(query)
    df.explain("codegen")
    df.show
  }

  test(s"Calcite-PPL basic query with nested UDF") {
    spark.udf.register("substring", (s: String, pos: Int) => s.substring(pos), StringType)

    val query =
      s"source = $testTable | eval f = SUBSTRING(SUBSTRING(name, 0 + 1), 1 + 1) | fields f"
    val df = sql(query)
    df.explain("codegen")
    df.show
  }

  test(s"Calcite-PPL basic query with all UDF registered") {

    val query = s"source = $testTable | eval f = LENGTH(name) | fields f"
    val df = sql(query)
    df.explain(true)
    df.show
  }
}
