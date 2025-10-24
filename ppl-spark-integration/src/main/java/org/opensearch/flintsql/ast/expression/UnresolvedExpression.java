/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flintsql.ast.expression;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.opensearch.flintsql.ast.AbstractNodeVisitor;
import org.opensearch.flintsql.ast.Node;

@EqualsAndHashCode(callSuper = false)
@ToString
public abstract class UnresolvedExpression extends Node {
  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitChildren(this, context);
  }
}
