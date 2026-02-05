/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.mockito.ArgumentMatchers.argThat
import org.mockito.Mockito.{verify, RETURNS_DEEP_STUBS}
import org.opensearch.flint.spark.query.UnifiedQuerySparkParser
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.parser.ParserInterface

class FlintUnifiedPPLSparkExtensionsSpec
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
    new FlintUnifiedPPLSparkExtensions()(extensions)
  }

  describe("inject parser") {
    it("should inject unified PPL parser") {
      verify(extensions).injectParser(argThat[SparkSessionExtensions#ParserBuilder] { builder =>
        builder(spark, sparkParser).isInstanceOf[UnifiedQuerySparkParser]
      })
    }
  }
}
