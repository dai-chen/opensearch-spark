/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.mockito.ArgumentMatchers.argThat
import org.mockito.Mockito.{verify, when, RETURNS_DEEP_STUBS}
import org.opensearch.flint.spark.ppl.FlintPPLConf.PPL_UNIFIED_ENABLED_KEY
import org.opensearch.flint.spark.ppl.FlintSparkPPLParser
import org.opensearch.flint.spark.query.UnifiedQuerySparkParser
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.parser.ParserInterface

class FlintPPLSparkExtensionsSpec
    extends AnyFunSpec
    with Matchers
    with MockitoSugar
    with BeforeAndAfterEach {

  private var extensions: SparkSessionExtensions = _
  private var spark: SparkSession = _
  private var sparkParser: ParserInterface = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    extensions = mock[SparkSessionExtensions]
    spark = mock[SparkSession](RETURNS_DEEP_STUBS)
    sparkParser = mock[ParserInterface]
    new FlintPPLSparkExtensions()(extensions)
  }

  describe("inject parser") {
    it("should inject legacy PPL parser by default") {
      when(spark.sparkContext.getConf.getAll)
        .thenReturn(Array.empty[(String, String)])

      verify(extensions).injectParser(argThat[SparkSessionExtensions#ParserBuilder] { builder =>
        builder(spark, sparkParser).isInstanceOf[FlintSparkPPLParser]
      })
    }

    it("should inject unified PPL parser when unified is enabled") {
      when(spark.sparkContext.getConf.getAll)
        .thenReturn(Array((PPL_UNIFIED_ENABLED_KEY, "true")))

      verify(extensions).injectParser(argThat[SparkSessionExtensions#ParserBuilder] { builder =>
        builder(spark, sparkParser).isInstanceOf[UnifiedQuerySparkParser]
      })
    }
  }
}
