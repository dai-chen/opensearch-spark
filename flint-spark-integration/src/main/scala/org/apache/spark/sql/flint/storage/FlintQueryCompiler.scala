/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint.storage

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, TimestampFormatter}
import org.apache.spark.sql.connector.expressions.{Expression, FieldReference, LiteralValue}
import org.apache.spark.sql.connector.expressions.filter.{And, Predicate}
import org.apache.spark.sql.flint.datatype.FlintDataType.STRICT_DATE_OPTIONAL_TIME_FORMATTER_WITH_NANOS
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * Todo. find the right package.
 */
case class FlintQueryCompiler(schema: StructType) {

  /**
   * Using AND to concat predicates. Todo. If spark spark.sql.ansi.enabled = true, more expression
   * defined in V2ExpressionBuilder could be pushed down.
   */
  def compile(predicates: Array[Predicate]): String = {
    if (predicates.isEmpty) {
      return ""
    }
    compile(predicates.reduce(new And(_, _)))
  }

  /**
   * Compile Expression to Flint query string.
   *
   * @param expr
   *   Expression.
   * @return
   *   empty if does not support.
   */
  def compile(expr: Expression, quoteString: Boolean = true): String = {
    expr match {
      case LiteralValue(value, dataType) =>
        quote(extract, quoteString)(value, dataType)
      case p: Predicate => visitPredicate(p)
      case f: FieldReference => f.toString()
      case _ => ""
    }
  }

  def extract(value: Any, dataType: DataType): String = dataType match {
    case TimestampType =>
      TimestampFormatter(
        STRICT_DATE_OPTIONAL_TIME_FORMATTER_WITH_NANOS,
        DateTimeUtils
          .getZoneId(SQLConf.get.sessionLocalTimeZone),
        false)
        .format(value.asInstanceOf[Long])
    case _ => Literal(value, dataType).toString()
  }

  def quote(f: ((Any, DataType) => String), quoteString: Boolean = true)(
      value: Any,
      dataType: DataType): String =
    dataType match {
      case DateType | TimestampType | StringType if quoteString =>
        s""""${f(value, dataType)}""""
      case _ => f(value, dataType)
    }

  /**
   * Predicate is defined in SPARK filters.scala. Todo.
   *   1. currently, we map spark contains to OpenSearch match query. Can we leverage more full
   *      text queries for text field. 2. configuration of expensive query.
   */
  def visitPredicate(p: Predicate): String = {
    val name = p.name()
    name match {
      case "IS_NULL" =>
        s"""{"bool":{"must_not":{"exists":{"field":"${compile(p.children()(0))}"}}}}"""
      case "IS_NOT_NULL" =>
        s"""{"exists":{"field":"${compile(p.children()(0))}"}}"""
      case "AND" =>
        s"""{"bool":{"filter":[${compile(p.children()(0))},${compile(p.children()(1))}]}}"""
      case "OR" =>
        s"""{"bool":{"should":[{"bool":{"filter":${compile(
            p.children()(0))}}},{"bool":{"filter":${compile(p.children()(1))}}}]}}"""
      case "NOT" =>
        s"""{"bool":{"must_not":${compile(p.children()(0))}}}"""
      case "=" =>
        s"""{"term":{"${compile(p.children()(0))}":{"value":${compile(p.children()(1))}}}}"""
      case ">" =>
        s"""{"range":{"${compile(p.children()(0))}":{"gt":${compile(p.children()(1))}}}}"""
      case ">=" =>
        s"""{"range":{"${compile(p.children()(0))}":{"gte":${compile(p.children()(1))}}}}"""
      case "<" =>
        s"""{"range":{"${compile(p.children()(0))}":{"lt":${compile(p.children()(1))}}}}"""
      case "<=" =>
        s"""{"range":{"${compile(p.children()(0))}":{"lte":${compile(p.children()(1))}}}}"""
      case "IN" =>
        val values = p.children().tail.map(expr => compile(expr)).mkString("[", ",", "]")
        s"""{"terms":{"${compile(p.children()(0))}":$values}}"""
      case "STARTS_WITH" =>
        s"""{"prefix":{"${compile(p.children()(0))}":{"value":${compile(p.children()(1))}}}}"""
      case "CONTAINS" =>
        val fieldName = compile(p.children()(0))
        if (isTextField(fieldName)) {
          s"""{"match":{"$fieldName":{"query":${compile(p.children()(1))}}}}"""
        } else {
          s"""{"wildcard":{"$fieldName":{"value":"*${compile(p.children()(1), false)}*"}}}"""
        }
      case "ENDS_WITH" =>
        s"""{"wildcard":{"${compile(p.children()(0))}":{"value":"*${compile(
            p.children()(1),
            false)}"}}}"""
      case "MIGHT_CONTAINS" =>
        s"""
           |{
           |    "bool": {
           |      "filter": {
           |        "script": {
           |          "script": {
           |            "lang": "painless",
           |            "source": "int hashLong(long input, int seed) {    int low = (int) input;    int high = (int) (input >>> 32);    int k1 = mixK1(low);    int h1 = mixH1(seed, k1);    k1 = mixK1(high);    h1 = mixH1(h1, k1);    return fmix(h1, 8);} int mixK1(int k1) {    k1 *= 0xcc9e2d51L;    k1 = Integer.rotateLeft(k1, 15);    k1 *= 0x1b873593L;    return k1;} int mixH1(int h1, int k1) {    h1 ^= k1;    h1 = Integer.rotateLeft(h1, 13);    h1 = h1 * 5 + (int) 0xe6546b64L;    return h1;} int fmix(int h1, int length) {    h1 ^= length;    h1 ^= h1 >>> 16;    h1 *= 0x85ebca6bL;    h1 ^= h1 >>> 13;    h1 *= 0xc2b2ae35L;    h1 ^= h1 >>> 16;    return h1;}BytesRef bf;Bytes = doc[params.fieldName].value;\nbyte[] buf = bfBytes.bytes;\nint pos = 0;\nint count = buf.length;\n// int version = dis.readInt();\nint ch1 = (pos < count) ? (buf[pos++] & (int) 0xffL) : -1;\nint ch2 = (pos < count) ? (buf[pos++] & (int) 0xffL) : -1;\nint ch3 = (pos < count) ? (buf[pos++] & (int) 0xffL) : -1;\nint ch4 = (pos < count) ? (buf[pos++] & (int) 0xffL) : -1;\nint version = ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));\n// int numHashFunctions = dis.readInt();\nch1 = (pos < count) ? (buf[pos++] & (int) 0xffL) : -1;\nch2 = (pos < count) ? (buf[pos++] & (int) 0xffL) : -1;\nch3 = (pos < count) ? (buf[pos++] & (int) 0xffL) : -1;\nch4 = (pos < count) ? (buf[pos++] & (int) 0xffL) : -1;\nint numHashFunctions = ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));\n// int numWords = dis.readInt();\nch1 = (pos < count) ? (buf[pos++] & (int) 0xffL) : -1;\nch2 = (pos < count) ? (buf[pos++] & (int) 0xffL) : -1;\nch3 = (pos < count) ? (buf[pos++] & (int) 0xffL) : -1;\nch4 = (pos < count) ? (buf[pos++] & (int) 0xffL) : -1;\nint numWords = ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));\n\n// Create BitArray internally\nlong[] data = new long[numWords];\nbyte[] readBuffer = new byte[8];\nfor (int i = 0; i < numWords; i++) {\n\n  // data[i] = dis.readLong()\n  int n = 0;\n  while (n < 8) {\n    int count2;\n    // int count2 = in.read(readBuffer, off + n, len - n);\n    int off = n;\n    int len = 8 - n;\n    if (pos >= count) {\n      count2 = -1;\n    } else {\n      int avail = count - pos;\n      if (len > avail) {\n        len = avail;\n      }\n      if (len <= 0) {\n        count2 = 0;\n      } else {\n        System.arraycopy(buf, pos, readBuffer, off, len);\n        pos += len;\n        count2 = len;\n      }\n    }\n    n += count2;\n  }\n  data[i] = (((long) readBuffer[0] << 56) +\n      ((long) (readBuffer[1] & 255) << 48) +\n      ((long) (readBuffer[2] & 255) << 40) +\n      ((long) (readBuffer[3] & 255) << 32) +\n      ((long) (readBuffer[4] & 255) << 24) +\n      ((readBuffer[5] & 255) << 16) +\n      ((readBuffer[6] & 255) << 8) +\n      ((readBuffer[7] & 255) << 0));\n}\nlong bitCount = 0;\nfor (long word : data) {\n  bitCount += Long.bitCount(word);\n}\n\n// BloomFilterImpl.mightContainLong(item)\nlong item = params.value;\nint h1 = hashLong(item, 0);\nint h2 = hashLong(item, h1);\n\nlong bitSize = (long) data.length * Long.SIZE;\nfor (int i = 1; i <= numHashFunctions; i++) {\n  int combinedHash = h1 + (i * h2);\n  // Flip all the bits if it'\''s negative (guaranteed positive number)\n  if (combinedHash < 0) {\n    combinedHash = ~combinedHash;\n  }\n  if ((data[(int) (combinedHash % bitSize >>> 6)] & (1L << combinedHash % bitSize)) == 0) {\n    return false;\n  }\n}\nreturn true",
           |            "params": {
           |              "fieldName": "${compile(p.children()(0))}",
           |              "value": ${compile(p.children()(1))}
           |            }
           |          }
           |        }
           |      }
           |    }
           |}
           |""".stripMargin
      case _ => ""
    }
  }

  /**
   * return true if the field is Flint Text field.
   */
  protected def isTextField(attribute: String): Boolean = {
    schema.apply(attribute) match {
      case StructField(_, StringType, _, metadata) =>
        metadata.contains("osType") && metadata.getString("osType") == "text"
      case _ => false
    }
  }
}
