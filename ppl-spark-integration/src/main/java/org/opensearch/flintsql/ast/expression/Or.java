/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flintsql.ast.expression;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.opensearch.flintsql.ast.AbstractNodeVisitor;

/** Expression node of the logic OR. */
@ToString
@EqualsAndHashCode(callSuper = true)
public class Or extends BinaryExpression {
  public Or(UnresolvedExpression left, UnresolvedExpression right) {
    super(left, right);
  }
  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitOr(this, context);
  }
}
