/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.calcite;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.interpreter.Context;
import org.apache.calcite.interpreter.Interpreter;
import org.apache.calcite.interpreter.JaninoRexCompiler;
import org.apache.calcite.interpreter.Scalar;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Util;
import org.apache.spark.internal.Logging;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.catalyst.expressions.codegen.CodeFormatter;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IClassBodyEvaluator;
import org.codehaus.commons.compiler.ICompilerFactory;
import org.codehaus.janino.ClassBodyEvaluator;
import org.codehaus.janino.SimpleCompiler;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class JaninoSparkUdfCompiler {
    private final RexBuilder rexBuilder;

    public JaninoSparkUdfCompiler(RexBuilder rexBuilder) {
        this.rexBuilder = rexBuilder;
    }

    @SuppressWarnings("unchecked")
    public UDF3<Object, Object, Object, Object> compile(
            List<RexNode> nodes,
            RelDataType inputRowType) {
        // 1) Build a RexProgram
        RexProgramBuilder programBuilder = new RexProgramBuilder(inputRowType, rexBuilder);
        for (RexNode node : nodes) {
            programBuilder.addProject(node, null);
        }
        RexProgram program = programBuilder.getProgram();

        // 2) Prepare code blocks
        BlockBuilder list       = new BlockBuilder();
        BlockBuilder staticList = new BlockBuilder().withRemoveUnused(false);

        // 3) Translator setup
        ParameterExpression contextExpr = Expressions.parameter(Context.class, "context");
        ParameterExpression outputExpr  = Expressions.parameter(Object[].class, "outputValues");
        JavaTypeFactoryImpl typeFactory =
                new JavaTypeFactoryImpl(rexBuilder.getTypeFactory().getTypeSystem());
        RexToLixTranslator.InputGetter getter = new RexToLixTranslator.InputGetterImpl(
                Expressions.field(contextExpr, "values"),
                PhysTypeImpl.of(typeFactory, inputRowType, JavaRowFormat.ARRAY, false)
        );
        Function1<String, RexToLixTranslator.InputGetter> correlates = s -> { throw new UnsupportedOperationException(); };

        // 4) Translate RexNodes into Lix expressions
        List<Expression> exprs = RexToLixTranslator.translateProjects(
                program,
                typeFactory,
                SqlConformanceEnum.DEFAULT,
                list,
                staticList,
                null,
                Expressions.field(contextExpr, "root"),
                getter,
                correlates
        );
        Ord.forEach(exprs, (expr, i) -> list.add(
                Expressions.statement(
                        Expressions.assign(
                                Expressions.arrayIndex(outputExpr, Expressions.constant(i)),
                                expr))
        ));

        // 5) Generate and return a UDF3 instance
        return udf3(list.toBlock(), staticList.toBlock().statements);
    }

    @SuppressWarnings("unchecked")
    private UDF3<Object, Object, Object, Object> udf3(
            BlockStatement body,
            List<Statement> preamble) {
        // 1) Build unique class name
        String pkg = this.getClass().getPackage().getName();
        String className = "CalciteUdf3_" + System.nanoTime();
        String fqcn = pkg + "." + className;

        // 2) Assemble Java source
        StringBuilder src = new StringBuilder();
        src.append("package ").append(pkg).append(";\n");
        src.append("import org.apache.spark.sql.api.java.UDF3;\n");
        src.append("import java.io.Serializable;\n");

        src.append("public class ").append(className)
                .append(" implements UDF3<Object,Object,Object,Object>, Serializable {\n");

        src.append("  private static final long serialVersionUID = 1L;\n\n");

        // Context inner class
        src.append("  public static class Context implements Serializable {\n");
        src.append("    private static final long serialVersionUID = 1L;\n");
        src.append("    public final Object[] values;\n");
        src.append("    public final Object root = null;\n");
        src.append("    public Context(Object a0, Object a1, Object a2) {\n");
        src.append("      values = new Object[13];\n");
        src.append("      values[0] = a0;\n");
        // src.append("      values[1] = a1;\n");
        // src.append("      values[2] = a2;\n");
        src.append("      // Map parameters to expected positions\n");
        src.append("      values[11] = a1;\n");
        src.append("      values[12] = a2;\n");
        src.append("    }\n");
        src.append("  }\n\n");

        src.append("  @Override public Object call(Object a0, Object a1, Object a2) throws Exception {\n");

        // 3) Inject preamble statements
        for (Statement stmt : preamble) {
            src.append("    ").append(stmt).append("\n");
        }

        // 4) Create context with the input parameters and output array
        src.append("    Context context = new Context(a0, a1, a2);\n");
        src.append("    Object[] outputValues = new Object[1];\n");

        // 5) Inject the body (assigns into outputValues[0])
        src.append("    // begin generated body\n");
        src.append(body).append("\n");
        src.append("    // end generated body\n");

        // 6) Return the computed value
        src.append("    return outputValues[0];\n");
        src.append("  }\n");
        src.append("}\n");

        // 7) Instead of compiling here, just pass the source to LazyCompilingUDF3
        System.err.println("Generated UDF source: " + src);
        return new LazyCompilingUDF3(fqcn, src.toString());
    }
}