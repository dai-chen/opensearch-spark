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
        String pkg       = this.getClass().getPackage().getName();
        String className = "CalciteUdf3_" + System.nanoTime();
        String fqcn      = pkg + "." + className;

        // 2) Assemble Java source
        StringBuilder src = new StringBuilder();
        src.append("package ").append(pkg).append(";\n");
        src.append("import org.apache.spark.sql.api.java.UDF3;\n");
        src.append("public class ").append(className)
                .append(" implements UDF3<Object,Object,Object,Object> {\n");
        src.append("  @Override public Object call(Object a0, Object a1, Object a2) throws Exception {\n");

        // 3) Inject preamble statements
        for (Statement stmt : preamble) {
            src.append("    ").append(stmt).append("\n");
        }

        // 4) Declare a values array
        src.append("    Object[] values = new Object[1];\n");

        // 5) Inject the body (assigns into values[0])
        src.append("    // begin generated body\n");
        src.append(body).append("\n");
        src.append("    // end generated body\n");

        // 6) Return the computed value
        src.append("    return values[0];\n");
        src.append("  }\n");
        src.append("}\n");

        // 7) Compile via Janino SimpleCompiler
        SimpleCompiler compiler = new SimpleCompiler();
        compiler.setParentClassLoader(getClass().getClassLoader());
        try {
            System.err.println("Compiled UDF code: " + src);
            compiler.cook(src.toString());
            Class<?> udfClass = compiler.getClassLoader().loadClass(fqcn);
            return (UDF3<Object, Object, Object, Object>) udfClass.getDeclaredConstructor().newInstance();
        } catch (CompileException | ReflectiveOperationException e) {
            throw new RuntimeException("Failed to compile UDF3 class " + fqcn, e);
        }
    }

    /*
    public UDF3 compile(List<RexNode> nodes,
                                   RelDataType inputRowType) {
        final RexProgramBuilder programBuilder =
                new RexProgramBuilder(inputRowType, rexBuilder);
        for (RexNode node : nodes) {
            programBuilder.addProject(node, null);
        }
        final RexProgram program = programBuilder.getProgram();

        final BlockBuilder list = new BlockBuilder();
        final BlockBuilder staticList = new BlockBuilder().withRemoveUnused(false);
        final ParameterExpression context_ =
                Expressions.parameter(Context.class, "context");
        final ParameterExpression outputValues_ =
                Expressions.parameter(Object[].class, "outputValues");
        final JavaTypeFactoryImpl javaTypeFactory =
                new JavaTypeFactoryImpl(rexBuilder.getTypeFactory().getTypeSystem());

        // public void execute(Context, Object[] outputValues)
        final RexToLixTranslator.InputGetter inputGetter =
                new RexToLixTranslator.InputGetterImpl(
                        Expressions.field(context_,
                                BuiltInMethod.CONTEXT_VALUES.field),
                        PhysTypeImpl.of(javaTypeFactory, inputRowType,
                                JavaRowFormat.ARRAY, false));
        final Function1<String, RexToLixTranslator.InputGetter> correlates = a0 -> {
            throw new UnsupportedOperationException();
        };
        final Expression root =
                Expressions.field(context_, BuiltInMethod.CONTEXT_ROOT.field);
        final SqlConformance conformance =
                SqlConformanceEnum.DEFAULT; // TODO: get this from implementor
        final List<Expression> expressionList =
                RexToLixTranslator.translateProjects(program, javaTypeFactory,
                        conformance, list, staticList, null, root, inputGetter, correlates);
        Ord.forEach(expressionList, (expression, i) ->
                list.add(
                        Expressions.statement(
                                Expressions.assign(
                                        Expressions.arrayIndex(outputValues_,
                                                Expressions.constant(i)),
                                        expression))));
        return udf3(context_, outputValues_, list.toBlock(),
                staticList.toBlock().statements);
    }

    private UDF3 udf3(ParameterExpression context_,
                      ParameterExpression outputValues_,
                      BlockStatement block,
                      List<Statement> declList) {
        final List<MemberDeclaration> declarations = new ArrayList<>();

        // Create UDF3 parameter expressions
        final ParameterExpression param1 = Expressions.parameter(Object.class, "t1");
        final ParameterExpression param2 = Expressions.parameter(Object.class, "t2");
        final ParameterExpression param3 = Expressions.parameter(Object.class, "t3");

        // Create context setup block for UDF call method
        final BlockBuilder callMethodBody = new BlockBuilder();

        // Create local array for input values
        Expression inputValues = callMethodBody.append("inputValues",
                Expressions.newArrayInit(Object.class,
                        ImmutableList.of(param1, param2, param3)));

        // Create a local array for output values
        Expression outputVals = callMethodBody.append("outputValues",
                Expressions.newArrayBounds(Object.class, 1, Expressions.constant(1)));

        // Create DataContext from parameters if needed
        Expression dataContext = callMethodBody.append("dataContext",
                Expressions.call(null,
                        BuiltInMethod.DATA_CONTEXT_GET_ROOT.method));

        // Create Context object from input values and dataContext
        Expression contextVar = callMethodBody.append("context",
                Expressions.new_(Context.class,
                        ImmutableList.of(dataContext, inputValues)));

        // Execute the original logic using our context and output values
        callMethodBody.add(
                Expressions.block(declList)); // Add static declarations first

        // Replace references to context_ and outputValues_ with our local variables
        BlockStatement modifiedBlock = RexToLixTranslator.replaceVariables(block,
                ImmutableMap.of(context_, contextVar, outputValues_, outputVals));

        callMethodBody.add(modifiedBlock);

        // Return the result
        callMethodBody.add(
                Expressions.return_(null,
                        Expressions.arrayIndex(outputVals, Expressions.constant(0))));

        // Add the UDF3.call method implementation
        declarations.add(
                Expressions.methodDecl(Modifier.PUBLIC, Object.class,
                        "call",
                        ImmutableList.of(param1, param2, param3),
                        callMethodBody.toBlock()));

        // Create class declaration implementing UDF3
        final ClassDeclaration classDeclaration =
                Expressions.classDecl(Modifier.PUBLIC, "SparkUdf3Implementation", null,
                        ImmutableList.of(UDF3.class), declarations);

        // Add toString method for better debugging
        declarations.add(
                Expressions.methodDecl(Modifier.PUBLIC, String.class,
                        "toString", ImmutableList.of(),
                        Expressions.block(
                                Expressions.return_(null,
                                        Expressions.constant("SparkUdf3Implementation")))));

        // Compile and instantiate the UDF
        String classCode = Expressions.toString(declarations, "\n", false);
        if (CalciteSystemProperty.DEBUG.value()) {
            Util.debugCode(System.out, classCode);
        }

        try {
            return compileUdf(classDeclaration, classCode);
        } catch (Exception e) {
            throw new RuntimeException("Failed to compile UDF3 implementation", e);
        }
    }

    private UDF3 compileUdf(ClassDeclaration decl, String classCode)
            throws CompileException, IOException {
        final ClassBodyEvaluator evaluator = new ClassBodyEvaluator();
        evaluator.setClassName(decl.name);
        evaluator.setImplementedInterfaces(new Class[] {UDF3.class});
        evaluator.setParentClassLoader(getClass().getClassLoader());

        // Generate Java code and compile it
        final Writer writer = new StringWriter();
        final CodeFormatter formatter = new CodeFormatter(writer);
        decl.accept(formatter);
        evaluator.cook(classCode);

        // Instantiate the generated class
        try {
            return (UDF3) evaluator.getClazz().getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException |
                 NoSuchMethodException | InvocationTargetException e) {
            throw new RuntimeException("Failed to instantiate generated UDF class", e);
        }
    }
     */
}