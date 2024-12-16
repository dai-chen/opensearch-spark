/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import java.io.File
import java.sql.Timestamp

import org.opensearch.flint.spark.mv.FlintSparkMaterializedView.getFlintIndexName
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.sql.flint.FlintDataSourceV2.FLINT_DATASOURCE

class FlintSparkMaterializedViewIntegrationsITSuite extends FlintSparkSuite with Matchers {

  test("create materialized view for VPC flow integration") {
    withIntegration("vpc_flow") { integration =>
      integration
        .createSourceTable(s"$catalogName.default.vpc_low_test")
        .createMaterializedView(s"""
             |SELECT
             |  TUMBLE(`@timestamp`, '5 Minute').start AS `start_time`,
             |  action AS `aws.vpc.action`,
             |  srcAddr AS `aws.vpc.srcaddr`,
             |  dstAddr AS `aws.vpc.dstaddr`,
             |  protocol AS `aws.vpc.protocol`,
             |  COUNT(*) AS `aws.vpc.total_count`,
             |  SUM(bytes) AS `aws.vpc.total_bytes`,
             |  SUM(packets) AS `aws.vpc.total_packets`
             |FROM (
             |  SELECT
             |    action,
             |    srcAddr,
             |    dstAddr,
             |    bytes,
             |    packets,
             |    protocol,
             |    CAST(FROM_UNIXTIME(start) AS TIMESTAMP) AS `@timestamp`
             |  FROM
             |    $catalogName.default.vpc_low_test
             |)
             |GROUP BY
             |  TUMBLE(`@timestamp`, '5 Minute'),
             |  action,
             |  srcAddr,
             |  dstAddr,
             |  protocol
             |""".stripMargin)
        .assertIndexData(
          Row(
            Timestamp.valueOf("2021-06-01 05:00:00"),
            "ACCEPT",
            "10.0.0.1",
            "10.0.0.2",
            6,
            2,
            350.0,
            15),
          Row(
            Timestamp.valueOf("2021-06-01 05:10:00"),
            "ACCEPT",
            "10.0.0.3",
            "10.0.0.4",
            6,
            1,
            300.0,
            15),
          Row(
            Timestamp.valueOf("2021-06-01 05:10:00"),
            "REJECT",
            "10.0.0.5",
            "10.0.0.6",
            6,
            1,
            400.0,
            20))
    }
  }

  test("create materialized view for CloudTrail integration") {
    withIntegration("cloud_trail") { integration =>
      integration
        .createSourceTable(s"$catalogName.default.cloud_trail_test")
        .createMaterializedView(s"""
             |SELECT
             |  TUMBLE(`@timestamp`, '5 Minute').start AS `start_time`,
             |  `userIdentity.type` AS `aws.cloudtrail.userIdentity.type`,
             |  `userIdentity.accountId` AS `aws.cloudtrail.userIdentity.accountId`,
             |  `userIdentity.sessionContext.sessionIssuer.userName` AS `aws.cloudtrail.userIdentity.sessionContext.sessionIssuer.userName`,
             |  `userIdentity.sessionContext.sessionIssuer.arn` AS `aws.cloudtrail.userIdentity.sessionContext.sessionIssuer.arn`,
             |  `userIdentity.sessionContext.sessionIssuer.type` AS `aws.cloudtrail.userIdentity.sessionContext.sessionIssuer.type`,
             |  awsRegion AS `aws.cloudtrail.awsRegion`,
             |  sourceIPAddress AS `aws.cloudtrail.sourceIPAddress`,
             |  eventSource AS `aws.cloudtrail.eventSource`,
             |  eventName AS `aws.cloudtrail.eventName`,
             |  eventCategory AS `aws.cloudtrail.eventCategory`,
             |  COUNT(*) AS `aws.cloudtrail.event_count`
             |FROM (
             |  SELECT
             |    CAST(eventTime AS TIMESTAMP) AS `@timestamp`,
             |    userIdentity.`type` AS `userIdentity.type`,
             |    userIdentity.`accountId` AS `userIdentity.accountId`,
             |    userIdentity.sessionContext.sessionIssuer.userName AS `userIdentity.sessionContext.sessionIssuer.userName`,
             |    userIdentity.sessionContext.sessionIssuer.arn AS `userIdentity.sessionContext.sessionIssuer.arn`,
             |    userIdentity.sessionContext.sessionIssuer.type AS `userIdentity.sessionContext.sessionIssuer.type`,
             |    awsRegion,
             |    sourceIPAddress,
             |    eventSource,
             |    eventName,
             |    eventCategory
             |  FROM
             |    $catalogName.default.cloud_trail_test
             |)
             |GROUP BY
             |  TUMBLE(`@timestamp`, '5 Minute'),
             |  `userIdentity.type`,
             |  `userIdentity.accountId`,
             |  `userIdentity.sessionContext.sessionIssuer.userName`,
             |  `userIdentity.sessionContext.sessionIssuer.arn`,
             |  `userIdentity.sessionContext.sessionIssuer.type`,
             |  awsRegion,
             |  sourceIPAddress,
             |  eventSource,
             |  eventName,
             |  eventCategory
             |""".stripMargin)
        .assertIndexData(Row(
          Timestamp.valueOf("2023-10-31 22:00:00"),
          "IAMUser",
          "123456789012",
          "MyRole",
          "arn:aws:iam::123456789012:role/MyRole",
          "Role",
          "us-east-1",
          "198.51.100.45",
          "sts.amazonaws.com",
          "AssumeRole",
          "Management",
          1))
    }
  }

  private def withIntegration(name: String)(codeBlock: IntegrationHelper => Unit): Unit = {
    withTempDir { checkpointDir =>
      val tableName = s"$catalogName.default.${name}_test"

      withTable(tableName) {
        codeBlock(new IntegrationHelper(name, tableName, checkpointDir))
      }
    }
  }

  private class IntegrationHelper(
      integrationName: String,
      tableName: String,
      checkpointDir: File) {
    private var mvName: String = _
    private var mvQuery: String = _

    def createSourceTable(tableName: String): IntegrationHelper = {
      val sqlTemplate = resourceToString(s"aws-logs/$integrationName.sql").mkString
      val sqlStatements =
        sqlTemplate
          .replace("{table_name}", tableName)
          .split(';')
          .map(_.trim)
          .filter(_.nonEmpty)

      sqlStatements.foreach(spark.sql)
      this
    }

    def createMaterializedView(mvQuery: String): IntegrationHelper = {
      this.mvName = s"$catalogName.default.${integrationName}_mv_test"
      this.mvQuery = mvQuery.replace("{table_name}", tableName)

      sql(s"""
           |CREATE MATERIALIZED VIEW $mvName
           |AS
           |${this.mvQuery}
           |WITH (
           |  auto_refresh = true,
           |  refresh_interval = '5 Seconds',
           |  watermark_delay = '1 Minute',
           |  checkpoint_location = '${checkpointDir.getAbsolutePath}'
           |)
           |""".stripMargin)

      val job = spark.streams.active
        .find(_.name == getFlintIndexName(mvName))
        .getOrElse(fail(s"Streaming job not found for integration: $integrationName"))
      failAfter(streamingTimeout) {
        job.processAllAvailable()
      }
      this
    }

    def assertIndexData(expectedRows: Row*): Unit = {
      val flintIndexName =
        spark.streams.active.find(_.name == getFlintIndexName(mvName)).get.name
      val actualRows = spark.read
        .format(FLINT_DATASOURCE)
        .options(openSearchOptions)
        .schema(sql(mvQuery).schema)
        .load(flintIndexName)

      checkAnswer(actualRows, expectedRows)
    }
  }
}
