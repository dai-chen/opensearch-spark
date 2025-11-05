/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.opensearch.flint.spark.query.catalog.SparkSchema
import org.opensearch.sql.data.model.ExprValueUtils
import org.opensearch.sql.expression.datetime.DateTimeFunctions

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.types.{IntegerType, StringType}

class FlintSparkPPLCalciteITSuite extends FlintSparkSuite {

  /** Test table and index name */
  private val testTable = "spark_catalog.default.flint_ppl_test"

  private val osCatalogName = "dev"

  /*
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
  }
   */

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Create test table
    createTimeSeriesTable(testTable)
  }

  ignore("test") {
    sql(s"CREATE TABLE spark_catalog.default.t1 (name STRING, age INT) USING JSON")
    sql("CREATE DATABASE test")
    withDatabase("test") {
      sql(s"CREATE TABLE spark_catalog.test.t2 (name STRING, age INT) USING JSON")

      val schema = new SparkSchema(spark, "")
      val t1 = schema.getSubSchema("default").getTable("t1")
      val t2 = schema.getSubSchema("test").getTable("t2")
    }
  }

  ignore("multi catalog") {
    sql("SHOW CATALOGS").show

    val indexName = "http_logs"
    withIndexName(indexName) {
      val mappings =
        """{
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

      createTimeSeriesTable("t1")

      sql("CREATE DATABASE test")
      withDatabase("test") {
        sql(s"CREATE TABLE spark_catalog.test.t2 (name STRING, age INT) USING JSON")

        val schema = new SparkSchema(spark, "spark_catalog")
        val t1 = schema.getSubSchema("default").getTable("t1")
        val t2 = schema.getSubSchema("test").getTable("t2")
        assertThrows[IllegalStateException] {
          val t3 = schema.getSubSchema("test").getTable("t3")
        }

        val osSchema = new SparkSchema(spark, osCatalogName)
        val i1 = osSchema.getSubSchema("default").getTable(indexName)
        assertThrows[IllegalStateException] {
          val i2 = osSchema.getSubSchema("default").getTable("not_exist_index")
        }
        assertThrows[IllegalStateException] {
          val i3 = osSchema.getSubSchema("not_exist_db").getTable(indexName)
        }

        sql("SHOW DATABASES").show
      }
    }
    sql("SHOW CATALOGS").show
  }

  ignore("Calcite-PPL basic query with OS index join S3") {
    val indexName = "http_logs"
    withIndexName(indexName) {
      val mappings =
        """{
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
             | source = $osCatalogName.default.$indexName|
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

  test("test PPL function resolved through Calcite - PPL function with same name") {
    val result = spark.sql(s"""
                              | source = $testTable
                              | | eval col1 = min('hello', 40, age)
                              | | fields name, col1
                              |""".stripMargin)

    result.explain(true)
    result.explain("codegen")
    result.show
  }

  test("test PPL function resolved through Calcite - PPL & Spark function mix use") {
    val result = spark.sql(s"""
                              | source = $testTable
                              | | eval col1 = COALESCE('hello', substring(name, 3)),
                              |        col2 = substring(COALESCE(name, 'world'), 3)
                              | | fields name, col1, col2
                              |""".stripMargin)

    result.explain(true)
    result.explain("codegen")
    result.show
  }

  test("test PPL function resolved through Calcite - PPL function only") {
    sql("CREATE TABLE test_calcite_func (name STRING, data STRING) USING JSON")
    sql("""INSERT INTO test_calcite_func VALUES
        ('alice', '{"age":25,"city":"NYC"}'),
        ('bob', '{"age":30,"city":"LA"}')""")

    val result = spark.sql("""
                             | source = spark_catalog.default.test_calcite_func
                             | | where name = 'alice'
                             | | eval cleaned_data = json_delete(data, 'age')
                             | | fields name, cleaned_data
                             |""".stripMargin)

    result.explain(true)
    result.show

    sql("DROP TABLE test_calcite_func")
  }

  // PPL bug: Unsupported function exception thrown in PPLFuncImpTable.resolve
  ignore("test PPL function resolved through Calcite - bin command") {
    val result = spark.sql(s"""
                              | source = $testTable
                              | | bin age span=3
                              |""".stripMargin)

    result.explain(true)
    result.explain("codegen")
    result.show
  }

  test("test PPL function resolved through Calcite - timechart command") {
    sql("CREATE TABLE test_events (`@timestamp` TIMESTAMP, host STRING, packets INT) USING JSON")
    sql("""INSERT INTO test_events VALUES
        (TIMESTAMP '2025-09-08 10:00:00', 'server1', 60),
        (TIMESTAMP '2025-09-08 10:01:00', 'server1', 120),
        (TIMESTAMP '2025-09-08 10:02:00', 'server1', 60),
        (TIMESTAMP '2025-09-08 10:02:30', 'server2', 180)""")

    val result = spark.sql("""
                             | source = spark_catalog.default.test_events
                             | | timechart span=2m limit=1 sum(packets) by host
                             |""".stripMargin)

    result.explain(true)
    result.show

    sql("DROP TABLE test_events")
  }

  test("test PPL function resolved through Calcite - spath command") {
    sql("CREATE TABLE test_calcite_func (name STRING, data STRING) USING JSON")
    sql("""INSERT INTO test_calcite_func VALUES
        ('alice', '{"age":25,"city":"NYC"}'),
        ('bob', '{"age":30,"city":"LA"}')""")

    val result = spark.sql("""
                             | source = spark_catalog.default.test_calcite_func
                             | | spath input=data city
                             | | fields name, city
                             |""".stripMargin)

    result.explain(true)
    result.show

    sql("DROP TABLE test_calcite_func")
  }

  test("test PPL aggregate function resolved through Calcite") {
    val result = spark.sql(s"""
                              | source = $testTable
                              | | stats values(age)
                              |""".stripMargin)

    result.explain(true)
    result.explain("codegen")
    result.show
  }

  // FIXME: SAFE_CAST should be translated to CAST in SparkSqlDialect
  ignore("test PPL schemaless support through Calcite") {
    sql("CREATE TABLE test_calcite_func (name STRING, data STRING) USING JSON")
    sql("""INSERT INTO test_calcite_func VALUES
        ('alice', '{"age":25,"city":"NYC"}'),
        ('bob', '{"age":30,"city":"LA"}')""")

    val result = spark.sql("""
                             | source = spark_catalog.default.test_calcite_func
                             | | spath input=data age
                             | | eval ageAbs = abs(age)
                             | | fields age, ageAbs
                             |""".stripMargin)

    result.explain(true)
    result.show

    sql("DROP TABLE test_calcite_func")
  }
}
