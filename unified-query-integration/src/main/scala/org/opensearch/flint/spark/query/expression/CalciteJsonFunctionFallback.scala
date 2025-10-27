/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query.expression

import com.fasterxml.jackson.databind.ObjectMapper

/**
 * Lightweight JSON helper used when Calcite-backed execution cannot evaluate JSON_* functions
 * directly. This mirrors the behaviour of the Spark-side implementations to keep Calcite feature
 * parity without introducing additional module dependencies.
 */
object CalciteJsonFunctionFallback {

  private val objectMapper = new ObjectMapper()

  /**
   * Remove the provided keys from the JSON string.
   */
  def jsonDelete(jsonStr: String, keysToRemove: Seq[String]): Option[String] = {
    if (jsonStr == null) {
      None
    } else {
      try {
        val jsonMap =
          objectMapper
            .readValue(jsonStr, classOf[java.util.LinkedHashMap[_, _]])
            .asInstanceOf[java.util.LinkedHashMap[String, Any]]

        keysToRemove
          .flatMap(key => Option(key).map(_.trim).filter(_.nonEmpty))
          .foreach { key =>
            val keyParts = key.split("\\.")
            removeNestedKey(jsonMap, keyParts, 0)
          }

        Some(objectMapper.writeValueAsString(jsonMap))
      } catch {
        case _: Exception =>
          None
      }
    }
  }

  private def removeNestedKey(currentObj: Any, keyParts: Array[String], depth: Int): Unit = {
    if (currentObj == null || depth >= keyParts.length) {
      return
    }

    currentObj match {
      case map: java.util.Map[_, _] =>
        val typedMap = map.asInstanceOf[java.util.Map[String, Any]]
        val currentKey = keyParts(depth)

        if (depth == keyParts.length - 1) {
          typedMap.remove(currentKey)
        } else if (typedMap.containsKey(currentKey)) {
          val nextObj = typedMap.get(currentKey)
          nextObj match {
            case list: java.util.List[_] =>
              val iterator = list.iterator()
              while (iterator.hasNext) {
                removeNestedKey(iterator.next(), keyParts, depth + 1)
              }
            case _ =>
              removeNestedKey(nextObj, keyParts, depth + 1)
          }
        }

      case _ => // No-op for non-map values
    }
  }
}

