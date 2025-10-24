/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flintsql.ast.tree;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.flintsql.ast.AbstractNodeVisitor;
import org.opensearch.flintsql.ast.Node;
import org.opensearch.flintsql.ast.expression.Field;
import org.opensearch.flintsql.ast.expression.UnresolvedExpression;

import java.util.List;
import java.util.Optional;

/** Logical plan node of Expand */
@RequiredArgsConstructor
public class Expand extends UnresolvedPlan {
  private UnresolvedPlan child;

  @Getter
  private final Field field;
  @Getter
  private final Optional<UnresolvedExpression> alias;
  
  @Override
  public Expand attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<? extends Node> getChild() {
    return child == null ? List.of() : List.of(child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitExpand(this, context);
  }
}
