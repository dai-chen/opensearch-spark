/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.bloomfilter

import java.io.ByteArrayInputStream

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.types._
import org.apache.spark.util.sketch.BloomFilter

case class BloomFilterMightContain(bloomFilterExpression: Expression, valueExpression: Expression)
    extends BinaryExpression {

  override def nullable: Boolean = true
  override def left: Expression = bloomFilterExpression
  override def right: Expression = valueExpression
  override def prettyName: String = "might_contain"
  override def dataType: DataType = BooleanType

  override def checkInputDataTypes(): TypeCheckResult = {
    (left.dataType, right.dataType) match {
      case (BinaryType, NullType) | (NullType, LongType) | (NullType, NullType) |
          (BinaryType, LongType) =>
        TypeCheckResult.TypeCheckSuccess
      case _ =>
        TypeCheckResult.TypeCheckFailure(s"Input to function $prettyName should have " +
          s"been ${BinaryType.simpleString} followed by a value with ${LongType.simpleString}, " +
          s"but it's [${left.dataType.catalogString}, ${right.dataType.catalogString}].")
    }
  }

  override protected def withNewChildrenInternal(
      newBloomFilterExpression: Expression,
      newValueExpression: Expression): BloomFilterMightContain =
    copy(bloomFilterExpression = newBloomFilterExpression, valueExpression = newValueExpression)

  override def eval(input: InternalRow): Any = {
    val bloomFilter = deserialize(bloomFilterExpression.eval(input).asInstanceOf[Array[Byte]])
    val value = valueExpression.eval(input)
    if (bloomFilter == null || value == null) {
      null
    } else {
      bloomFilter.mightContainLong(value.asInstanceOf[Long])
    }
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
