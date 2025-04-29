/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.flintsql.ast.tree;

import org.opensearch.flintsql.ast.expression.Literal;

import java.util.Optional;

/**
 * marker interface for numeric based count aggregation (specific number of returned results)
 */
public interface CountedAggregation {
    Optional<Literal> getResults();
}
