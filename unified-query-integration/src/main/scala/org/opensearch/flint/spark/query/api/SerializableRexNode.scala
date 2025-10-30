/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query.api

import java.lang.reflect.Type
import java.util

import scala.collection.JavaConverters._

import com.google.common.collect.ImmutableList
import org.apache.calcite.DataContext
import org.apache.calcite.adapter.enumerable.{EnumUtils, RexToLixTranslator}
import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.config.CalciteSystemProperty
import org.apache.calcite.jdbc.JavaTypeFactoryImpl
import org.apache.calcite.linq4j.QueryProvider
import org.apache.calcite.linq4j.function.Function1
import org.apache.calcite.linq4j.tree.{BlockBuilder, Expressions, LabelTarget, ParameterExpression}
import org.apache.calcite.plan.{RelOptCluster, RelOptPlanner}
import org.apache.calcite.plan.volcano.VolcanoPlanner
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeField}
import org.apache.calcite.rex.{RexBuilder, RexCall, RexExecutable, RexInputRef, RexNode, RexProgram, RexProgramBuilder}
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.sql.validate.{SqlConformance, SqlConformanceEnum}
import org.apache.calcite.util.BuiltInMethod
import org.apache.calcite.util.Util
import org.opensearch.sql.opensearch.storage.serde.RelJsonSerializer

/**
 * A serializable wrapper for RexNode that delegates serialization/deserialization to OpenSearch's
 * RelJsonSerializer for safe distribution across executors.
 *
 * RelJsonSerializer handles all the complexity of Calcite's JSON-based serialization, including
 * proper InputTranslator usage for RexInputRef reconstruction.
 */
class SerializableRexNode(@transient private var _rexNode: RexNode) extends Serializable {

  // Serialized form
  private var serializedData: String = _

  @transient private lazy val typeFactory: JavaTypeFactory = new JavaTypeFactoryImpl()
  @transient private lazy val rexBuilder: RexBuilder = new RexBuilder(typeFactory)
  @transient private lazy val planner: RelOptPlanner = new VolcanoPlanner()
  @transient private lazy val cluster: RelOptCluster = {
    planner.setExecutor(null)
    RelOptCluster.create(planner, rexBuilder)
  }
  @transient private lazy val relJsonSerializer: RelJsonSerializer = new RelJsonSerializer(
    cluster)
  @transient private lazy val rowType: RelDataType = extractInputSchema(_rexNode)
  @transient private lazy val evaluationFunction: Function1[DataContext, Array[AnyRef]] =
    createEvaluationFunction()

  def getRexNode: RexNode = _rexNode

  def evaluate(inputs: Seq[Any]): Any = {
    val dataContext =
      new SerializableRexNode.InMemoryDataContext(
        buildInputValueMap(inputs, rowType),
        typeFactory)
    val result = evaluationFunction.apply(dataContext)
    if (result == null || result.isEmpty) null else result(0)
  }

  private def buildInputValueMap(
      inputs: Seq[Any],
      inputRowType: RelDataType): java.util.Map[String, Any] = {
    val valueMap = new util.HashMap[String, Any]()
    val fields = inputRowType.getFieldList.asScala
    fields.zipWithIndex.foreach { case (field: RelDataTypeField, idx) =>
      val value = if (idx < inputs.length) inputs(idx) else null
      valueMap.put(field.getName, value)
    }
    valueMap.put(DataContext.Variable.UTC_TIMESTAMP.camelName, System.currentTimeMillis())
    valueMap
  }

  private def createEvaluationFunction(): Function1[DataContext, Array[AnyRef]] = {
    val getter = new SerializableRexNode.DefaultInputGetter(typeFactory, rowType)
    val code = SerializableRexNode.translate(
      rexBuilder,
      java.util.Collections.singletonList(_rexNode),
      getter,
      rowType)
    new RexExecutable(code, "UnifiedFunctionRexExecutable").getFunction
      .asInstanceOf[Function1[DataContext, Array[AnyRef]]]
  }

  /**
   * Extract input schema (RelDataType) from RexNode by collecting all RexInputRef types.
   */
  private def extractInputSchema(rexNode: RexNode): RelDataType = {
    import scala.collection.mutable

    val inputRefs = mutable.Map[Int, RelDataType]()

    def collectInputRefs(node: RexNode): Unit = {
      node match {
        case inputRef: RexInputRef =>
          inputRefs(inputRef.getIndex) = inputRef.getType
        case call: RexCall =>
          call.getOperands.asScala.foreach(collectInputRefs)
        case _ => // Other node types don't have inputs
      }
    }

    collectInputRefs(rexNode)

    // Build RelDataType with collected input types
    if (inputRefs.isEmpty) {
      typeFactory.createStructType(
        java.util.Collections.emptyList(),
        java.util.Collections.emptyList())
    } else {
      val sortedInputs = inputRefs.toSeq.sortBy(_._1)
      val types = sortedInputs.map(_._2).asJava
      val names = sortedInputs.map(idx => s"_${idx._1}").asJava
      typeFactory.createStructType(types, names)
    }
  }

  /**
   * Serialize RexNode using RelJsonSerializer. fieldTypes parameter is empty since it's
   * OpenSearch-specific (ExprType).
   */
  private def serialize(rexNode: RexNode): String = {
    try {
      relJsonSerializer.serialize(rexNode, rowType, java.util.Collections.emptyMap())
    } catch {
      case e: Exception =>
        throw new IllegalStateException(s"Failed to serialize RexNode: $rexNode", e)
    }
  }

  /**
   * Deserialize RexNode using RelJsonSerializer. Extracts the RexNode from the deserialized map.
   */
  private def deserialize(struct: String): RexNode = {
    try {
      val resultMap = relJsonSerializer.deserialize(struct)
      resultMap.get(RelJsonSerializer.EXPR).asInstanceOf[RexNode]
    } catch {
      case e: Exception =>
        throw new IllegalStateException(s"Failed to deserialize RexNode: $struct", e)
    }
  }

  @throws(classOf[java.io.IOException])
  private def writeObject(out: java.io.ObjectOutputStream): Unit = {
    serializedData = serialize(_rexNode)
    out.defaultWriteObject()
  }

  @throws(classOf[java.io.IOException])
  @throws(classOf[ClassNotFoundException])
  private def readObject(in: java.io.ObjectInputStream): Unit = {
    in.defaultReadObject()
    _rexNode = deserialize(serializedData)
  }
}

object SerializableRexNode {

  private class DefaultInputGetter(typeFactory: JavaTypeFactory, rowType: RelDataType)
      extends RexToLixTranslator.InputGetter {

    override def field(
        list: BlockBuilder,
        index: Int,
        storageType: Type): org.apache.calcite.linq4j.tree.Expression = {
      val field = rowType.getFieldList.get(index)
      val expectedType =
        if (storageType == null) typeFactory.getJavaClass(field.getType) else storageType
      val fieldAccess = Expressions.call(
        DataContext.ROOT,
        BuiltInMethod.DATA_CONTEXT_GET.method,
        Expressions.constant(field.getName))
      EnumUtils.convert(fieldAccess, expectedType)
    }
  }

  private class InMemoryDataContext(
      valueByField: java.util.Map[String, Any],
      typeFactory: JavaTypeFactory)
      extends DataContext {

    override def getRootSchema: SchemaPlus = null

    override def getTypeFactory: JavaTypeFactory = typeFactory

    override def getQueryProvider: QueryProvider = null

    override def get(name: String): AnyRef =
      valueByField.get(name).asInstanceOf[AnyRef]
  }

  private[api] def translate(
      rexBuilder: RexBuilder,
      constExps: java.util.List[RexNode],
      getter: RexToLixTranslator.InputGetter,
      rowType: RelDataType): String = {
    val programBuilder = new RexProgramBuilder(rowType, rexBuilder)
    val iterator = constExps.iterator()
    while (iterator.hasNext) {
      val node = iterator.next()
      programBuilder.addProject(node, s"c${programBuilder.getProjectList.size}")
    }

    val typeFactory = rexBuilder.getTypeFactory
    val javaTypeFactory =
      typeFactory match {
        case jf: JavaTypeFactory => jf
        case _ => new JavaTypeFactoryImpl(typeFactory.getTypeSystem)
      }

    val blockBuilder = new BlockBuilder()
    val root0: ParameterExpression = Expressions.parameter(classOf[Object], "root0")
    val root: ParameterExpression = DataContext.ROOT
    blockBuilder.add(
      Expressions.declare(16, root, Expressions.convert_(root0, classOf[DataContext])))

    val conformance: SqlConformance = SqlConformanceEnum.DEFAULT
    val program: RexProgram = programBuilder.getProgram
    val expressions =
      RexToLixTranslator.translateProjects(
        program,
        javaTypeFactory,
        conformance,
        blockBuilder,
        null,
        null,
        root,
        getter,
        null)

    blockBuilder.add(
      Expressions.return_(
        null.asInstanceOf[LabelTarget],
        Expressions.newArrayInit(classOf[Array[Object]], expressions)))

    val methodDecl =
      Expressions.methodDecl(
        1,
        classOf[Array[Object]],
        BuiltInMethod.FUNCTION1_APPLY.method.getName,
        ImmutableList.of(root0),
        blockBuilder.toBlock())

    Expressions.toString(methodDecl)
  }
}
