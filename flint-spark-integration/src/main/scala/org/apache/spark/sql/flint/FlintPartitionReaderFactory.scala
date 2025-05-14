/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.flint.storage.FlintQueryCompiler
import org.apache.spark.sql.types.StructType
import org.json.JSONObject

case class FlintPartitionReaderFactory(
    schema: StructType,
    options: FlintSparkConf,
    pushedPredicates: Array[Predicate])
    extends PartitionReaderFactory
    with Logging {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val dsl = Option(options.dsl)
    val query =
      if (dsl.isEmpty) {
        FlintQueryCompiler(schema).compile(pushedPredicates)
      } else {
        // Already done by Calcite
        new JSONObject(dsl.get).getJSONObject("query").toString
      }
    logInfo(s"Executing DSL query: $query")

    new FlintPartitionReader(
      partition.asInstanceOf[OpenSearchSplit].table.createReader(query),
      schema,
      options)
  }
}
