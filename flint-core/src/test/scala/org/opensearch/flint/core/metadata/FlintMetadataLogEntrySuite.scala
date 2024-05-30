/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metadata

import com.stephenn.scalatest.jsonassert.JsonMatchers.matchJson
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization
import org.opensearch.flint.core.metadata.log.FlintMetadataLogEntry
import org.opensearch.flint.core.metadata.log.FlintMetadataLogEntry.IndexState.FAILED
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FlintMetadataLogEntrySuite extends AnyFlatSpec with Matchers {

  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  "toJson" should "correctly serialize an entry with special characters" in {
    val logEntry = FlintMetadataLogEntry(
      "id",
      1,
      2,
      1627890000000L,
      FAILED,
      "dataSource",
      s"""Error with quotes ' " and newline: \n and tab: \t characters""")

    val actualJson = logEntry.toJson
    val lastUpdateTime = (parse(actualJson) \ "lastUpdateTime").extract[Long]
    val expectedJson =
      s"""
         |{
         |  "version": "1.0",
         |  "latestId": "id",
         |  "type": "flintindexstate",
         |  "state": "failed",
         |  "applicationId": "unknown",
         |  "jobId": "unknown",
         |  "dataSourceName": "dataSource",
         |  "jobStartTime": 1627890000000,
         |  "lastUpdateTime": $lastUpdateTime,
         |  "error": "Error with quotes ' \\" and newline: \\n and tab: \\t characters"
         |}
         |""".stripMargin
    actualJson should matchJson(expectedJson)
  }
}
