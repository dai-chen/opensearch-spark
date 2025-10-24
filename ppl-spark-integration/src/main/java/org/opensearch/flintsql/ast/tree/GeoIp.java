/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flintsql.ast.tree;

import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.flintsql.ast.AbstractNodeVisitor;
import org.opensearch.flintsql.ast.Node;
import org.opensearch.flintsql.ast.expression.AttributeList;
import org.opensearch.flintsql.ast.expression.Field;
import org.opensearch.flintsql.ast.expression.UnresolvedExpression;

import java.util.List;

@Getter
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class GeoIp extends UnresolvedPlan {
    private UnresolvedPlan child;
    private final Field field;
    private final UnresolvedExpression ipAddress;
    private final AttributeList properties;

    @Override
    public List<? extends Node> getChild() {
        return ImmutableList.of(child);
    }

    @Override
    public <T,C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
        return nodeVisitor.visitGeoIp(this, context);
    }

    @Override
    public UnresolvedPlan attach(UnresolvedPlan child) {
        this.child = child;
        return this;
    }
}