/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.legacy.ast.tree;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.legacy.ast.AbstractNodeVisitor;
import org.opensearch.legacy.ast.Node;
import org.opensearch.legacy.ast.expression.Field;

import java.util.List;
import org.opensearch.legacy.ast.expression.UnresolvedExpression;

@RequiredArgsConstructor
public class Flatten extends UnresolvedPlan {

    private UnresolvedPlan child;

    @Getter
    private final Field field;
    @Getter
    private final List<UnresolvedExpression> aliasSequence;

    @Override
    public UnresolvedPlan attach(UnresolvedPlan child) {
        this.child = child;
        return this;
    }

    @Override
    public List<? extends Node> getChild() {
        return child == null ? List.of() : List.of(child);
    }
    
    @Override
    public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
        return nodeVisitor.visitFlatten(this, context);
    }
}
