/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query.expression

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper

import org.apache.spark.SparkFunSuite

class CalciteJsonFunctionFallbackSuite extends SparkFunSuite {

  private val objectMapper = new ObjectMapper()

  test("jsonDelete removes top-level key and preserves others") {
    val json = """{"name":"alice","age":25,"city":"NYC"}"""
    val result = CalciteJsonFunctionFallback.jsonDelete(json, Seq("age"))

    assert(result.isDefined)
    val node = objectMapper.readTree(result.get)
    assert(node.has("name"))
    assert(node.has("city"))
    assert(!node.has("age"))
  }

  test("jsonDelete removes nested keys within arrays") {
    val json =
      """{"items":[{"id":1,"metrics":{"latency":10}},{"id":2,"metrics":{"latency":20}}]}"""
    val result = CalciteJsonFunctionFallback.jsonDelete(json, Seq("items.metrics.latency"))

    assert(result.isDefined)
    val array = objectMapper.readTree(result.get).get("items")
    val containsLatency =
      array.elements().asScala.exists(item => item.path("metrics").has("latency"))
    assert(!containsLatency)
  }
}
