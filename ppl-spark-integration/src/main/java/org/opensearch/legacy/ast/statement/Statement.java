/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.legacy.ast.statement;

import org.opensearch.legacy.ast.AbstractNodeVisitor;
import org.opensearch.legacy.ast.Node;

/** Statement is the high interface of core engine. */
public abstract class Statement extends Node {
  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> visitor, C context) {
    return visitor.visitStatement(this, context);
  }
}
