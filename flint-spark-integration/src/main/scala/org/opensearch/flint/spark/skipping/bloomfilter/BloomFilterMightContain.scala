/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.bloomfilter

import java.io.ByteArrayInputStream

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{BinaryComparison, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.types._
import org.apache.spark.util.sketch.BloomFilter

/**
 * An internal scalar function that returns the membership check result (either true or false) for
 * values of `valueExpression` in the Bloom filter represented by `bloomFilterExpression`. Not
 * that since the function is "might contain", always returning true regardless is not wrong. Note
 * that this expression requires that `bloomFilterExpression` is either a constant value or an
 * uncorrelated scalar subquery. This is sufficient for the Bloom filter join rewrite.
 *
 * @param bloomFilterExpression
 *   the Binary data of Bloom filter.
 * @param valueExpression
 *   the Long value to be tested for the membership of `bloomFilterExpression`.
 */
case class BloomFilterMightContain(bloomFilterExpression: Expression, valueExpression: Expression)
    extends BinaryComparison {

  override def nullable: Boolean = true
  override def left: Expression = bloomFilterExpression
  override def right: Expression = valueExpression
  override def prettyName: String = "might_contains"
  override def dataType: DataType = BooleanType

  override def symbol: String = "MIGHT_CONTAINS"

  override def checkInputDataTypes(): TypeCheckResult = TypeCheckResult.TypeCheckSuccess

  override protected def withNewChildrenInternal(
      newBloomFilterExpression: Expression,
      newValueExpression: Expression): BloomFilterMightContain =
    copy(bloomFilterExpression = newBloomFilterExpression, valueExpression = newValueExpression)

  override def eval(input: InternalRow): Any = {
    val bloomFilter = deserialize(bloomFilterExpression.eval(input).asInstanceOf[Array[Byte]])
    val value = valueExpression.eval(input)
    if (value == null) null else bloomFilter.mightContainLong(value.asInstanceOf[Long])
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val bloomFilterEval = bloomFilterExpression.genCode(ctx)
    val valueEval = valueExpression.genCode(ctx)

    val bf = ctx.addReferenceObj("bloomFilter", null, classOf[BloomFilter].getName)
    val code =
      code"""
            |${bloomFilterEval.code}
            |${valueEval.code}
            |boolean ${ev.isNull} = ${bloomFilterEval.isNull} || ${valueEval.isNull};
            |${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(
             dataType)};
            |if (!${ev.isNull}) {
            |  java.io.ByteArrayInputStream in = new java.io.ByteArrayInputStream(${bloomFilterEval.value});
            |  org.apache.spark.util.sketch.BloomFilter bloomFilter = org.apache.spark.util.sketch.BloomFilter.readFrom(in);
            |  in.close();
            |
            |  if ($bf != null) {
            |    ${ev.value} = $bf.mightContainLong((${CodeGenerator.boxedType(
             valueExpression.dataType)})${valueEval.value});
            |  } else {
            |    ${ev.isNull} = true;
            |  }
            |}
            |""".stripMargin

    ev.copy(code = code)
  }

  final def deserialize(bytes: Array[Byte]): BloomFilter = {
    val in = new ByteArrayInputStream(bytes)
    val bloomFilter = BloomFilter.readFrom(in)
    in.close()
    bloomFilter
  }
}
