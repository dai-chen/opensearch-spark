/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.legacy.ast.tree;

import org.opensearch.legacy.ast.expression.Literal;

import java.util.Optional;

/**
 * marker interface for numeric based count aggregation (specific number of returned results)
 */
public interface CountedAggregation {
    Optional<Literal> getResults();
}
