/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flintsql.ast;

import org.opensearch.flintsql.ast.expression.AggregateFunction;
import org.opensearch.flintsql.ast.expression.Alias;
import org.opensearch.flintsql.ast.expression.AllFields;
import org.opensearch.flintsql.ast.expression.And;
import org.opensearch.flintsql.ast.expression.Argument;
import org.opensearch.flintsql.ast.expression.AttributeList;
import org.opensearch.flintsql.ast.expression.Between;
import org.opensearch.flintsql.ast.expression.Case;
import org.opensearch.flintsql.ast.expression.Cast;
import org.opensearch.flintsql.ast.expression.Cidr;
import org.opensearch.flintsql.ast.expression.Compare;
import org.opensearch.flintsql.ast.expression.EqualTo;
import org.opensearch.flintsql.ast.expression.Field;
import org.opensearch.flintsql.ast.expression.FieldList;
import org.opensearch.flintsql.ast.expression.LambdaFunction;
import org.opensearch.flintsql.ast.tree.*;
import org.opensearch.flintsql.ast.expression.FieldsMapping;
import org.opensearch.flintsql.ast.expression.Function;
import org.opensearch.flintsql.ast.expression.In;
import org.opensearch.flintsql.ast.expression.subquery.ExistsSubquery;
import org.opensearch.flintsql.ast.expression.subquery.InSubquery;
import org.opensearch.flintsql.ast.expression.Interval;
import org.opensearch.flintsql.ast.expression.IsEmpty;
import org.opensearch.flintsql.ast.expression.Let;
import org.opensearch.flintsql.ast.expression.Literal;
import org.opensearch.flintsql.ast.expression.Map;
import org.opensearch.flintsql.ast.expression.Not;
import org.opensearch.flintsql.ast.expression.Or;
import org.opensearch.flintsql.ast.expression.QualifiedName;
import org.opensearch.flintsql.ast.expression.subquery.ScalarSubquery;
import org.opensearch.flintsql.ast.expression.Span;
import org.opensearch.flintsql.ast.expression.UnresolvedArgument;
import org.opensearch.flintsql.ast.expression.UnresolvedAttribute;
import org.opensearch.flintsql.ast.expression.When;
import org.opensearch.flintsql.ast.expression.WindowFunction;
import org.opensearch.flintsql.ast.expression.Xor;
import org.opensearch.flintsql.ast.statement.Explain;
import org.opensearch.flintsql.ast.statement.Query;
import org.opensearch.flintsql.ast.statement.Statement;
import org.opensearch.flintsql.ast.tree.*;

/** AST nodes visitor Defines the traverse path. */
public abstract class AbstractNodeVisitor<T, C> {

  public T visit(Node node, C context) {
    return null;
  }

  /**
   * Visit child node.
   *
   * @param node {@link Node}
   * @param context Context
   * @return Return Type.
   */
  public T visitChildren(Node node, C context) {
    T result = defaultResult();

    for (Node child : node.getChild()) {
      T childResult = child.accept(this, context);
      result = aggregateResult(result, childResult);
    }
    return result;
  }

  private T defaultResult() {
    return null;
  }

  private T aggregateResult(T aggregate, T nextResult) {
    return nextResult;
  }

  public T visitRelation(Relation node, C context) {
    return visitChildren(node, context);
  }

  public T visitTableFunction(TableFunction node, C context) {
    return visitChildren(node, context);
  }

  public T visitFilter(Filter node, C context) {
    return visitChildren(node, context);
  }

  public T visitExpand(Expand node, C context) {
    return visitChildren(node, context);
  }

  public T visitLookup(Lookup node, C context) {
    return visitChildren(node, context);
  }

  public T visitTrendline(Trendline node, C context) {
    return visitChildren(node, context);
  }

  public T visitAppendCol(AppendCol node, C context) {
    return visitChildren(node, context);
  }

  public T visitCorrelation(Correlation node, C context) {
    return visitChildren(node, context);
  }

  public T visitCorrelationMapping(FieldsMapping node, C context) {
    return visitChildren(node, context);
  }

  public T visitJoin(Join node, C context) {
    return visitChildren(node, context);
  }

  public T visitSubqueryAlias(SubqueryAlias node, C context) {
    return visitChildren(node, context);
  }

  public T visitProject(Project node, C context) {
    return visitChildren(node, context);
  }

  public T visitAggregation(Aggregation node, C context) {
    return visitChildren(node, context);
  }

  public T visitEqualTo(EqualTo node, C context) {
    return visitChildren(node, context);
  }

  public T visitLiteral(Literal node, C context) {
    return visitChildren(node, context);
  }

  public T visitUnresolvedAttribute(UnresolvedAttribute node, C context) {
    return visitChildren(node, context);
  }

  public T visitAttributeList(AttributeList node, C context) {
    return visitChildren(node, context);
  }

  public T visitMap(Map node, C context) {
    return visitChildren(node, context);
  }

  public T visitNot(Not node, C context) {
    return visitChildren(node, context);
  }

  public T visitOr(Or node, C context) {
    return visitChildren(node, context);
  }

  public T visitAnd(And node, C context) {
    return visitChildren(node, context);
  }

  public T visitXor(Xor node, C context) {
    return visitChildren(node, context);
  }

  public T visitAggregateFunction(AggregateFunction node, C context) {
    return visitChildren(node, context);
  }

  public T visitFunction(Function node, C context) {
    return visitChildren(node, context);
  }

  public T visitCast(Cast node, C context) {
    return visitChildren(node, context);
  }

  public T visitLambdaFunction(LambdaFunction node, C context) {
    return visitChildren(node, context);
  }

  public T visitIsEmpty(IsEmpty node, C context) {
    return visitChildren(node, context);
  }

  // TODO add case

  public T visitWindowFunction(WindowFunction node, C context) {
    return visitChildren(node, context);
  }

  public T visitIn(In node, C context) {
    return visitChildren(node, context);
  }

  public T visitCompare(Compare node, C context) {
    return visitChildren(node, context);
  }

  public T visitBetween(Between node, C context) {
    return visitChildren(node, context);
  }

  public T visitArgument(Argument node, C context) {
    return visitChildren(node, context);
  }

  public T visitField(Field node, C context) {
    return visitChildren(node, context);
  }

  public T visitFieldList(FieldList node, C context) {
    return visitChildren(node, context);
  }

  public T visitQualifiedName(QualifiedName node, C context) {
    return visitChildren(node, context);
  }

  public T visitRename(Rename node, C context) {
    return visitChildren(node, context);
  }

  public T visitEval(Eval node, C context) {
    return visitChildren(node, context);
  }

  public T visitParse(Parse node, C context) {
    return visitChildren(node, context);
  }

  public T visitLet(Let node, C context) {
    return visitChildren(node, context);
  }

  public T visitSort(Sort node, C context) {
    return visitChildren(node, context);
  }

  public T visitDedupe(Dedupe node, C context) {
    return visitChildren(node, context);
  }

  public T visitHead(Head node, C context) {
    return visitChildren(node, context);
  }

  public T visitRareTopN(RareTopN node, C context) {
    return visitChildren(node, context);
  }
  public T visitValues(Values node, C context) {
    return visitChildren(node, context);
  }

  public T visitAlias(Alias node, C context) {
    return visitChildren(node, context);
  }

  public T visitAllFields(AllFields node, C context) {
    return visitChildren(node, context);
  }

  public T visitInterval(Interval node, C context) {
    return visitChildren(node, context);
  }

  public T visitCase(Case node, C context) {
    return visitChildren(node, context);
  }

  public T visitWhen(When node, C context) {
    return visitChildren(node, context);
  }

  public T visitUnresolvedArgument(UnresolvedArgument node, C context) {
    return visitChildren(node, context);
  }

  public T visitLimit(Limit node, C context) {
    return visitChildren(node, context);
  }

  public T visitSpan(Span node, C context) {
    return visitChildren(node, context);
  }

  public T visitKmeans(Kmeans node, C context) {
    return visitChildren(node, context);
  }

  public T visitStatement(Statement node, C context) {
    return visit(node, context);
  }

  public T visitQuery(Query node, C context) {
    return visitStatement(node, context);
  }

  public T visitExplain(Explain node, C context) {
    return visitStatement(node, context);
  }

  public T visitInSubquery(InSubquery node, C context) {
    return visitChildren(node, context);
  }
  
  public T visitFillNull(FillNull fillNull, C context) {
    return visitChildren(fillNull, context);
  }
  
  public T visitFieldSummary(FieldSummary fieldSummary, C context) {
    return visitChildren(fieldSummary, context);
  }

  public T visitScalarSubquery(ScalarSubquery node, C context) {
    return visitChildren(node, context);
  }

  public T visitExistsSubquery(ExistsSubquery node, C context) {
    return visitChildren(node, context);
  }

  public T visitWindow(Window node, C context) {
    return visitChildren(node, context);
  }

  public T visitCidr(Cidr node, C context) {
    return visitChildren(node, context);
  }

  public T visitGeoIp(GeoIp node, C context) {
    return visitChildren(node, context);
  }

  public T visitFlatten(Flatten flatten, C context) {
    return visitChildren(flatten, context);
  }
}
