/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.legacy.ast.expression.subquery;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.legacy.ast.AbstractNodeVisitor;
import org.opensearch.legacy.ast.expression.UnresolvedExpression;
import org.opensearch.legacy.ast.tree.UnresolvedPlan;

@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class ScalarSubquery extends UnresolvedExpression {
    private final UnresolvedPlan query;

    @Override
    public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
        return nodeVisitor.visitScalarSubquery(this, context);
    }
}
