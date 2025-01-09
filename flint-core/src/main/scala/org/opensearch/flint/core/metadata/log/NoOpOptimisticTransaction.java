/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metadata.log;

import org.opensearch.flint.common.metadata.log.FlintMetadataLog;
import org.opensearch.flint.common.metadata.log.FlintMetadataLogEntry;
import org.opensearch.flint.common.metadata.log.OptimisticTransaction;
import org.opensearch.flint.core.storage.FlintOpenSearchMetadataLog;

import java.util.function.Function;
import java.util.function.Predicate;

public class NoOpOptimisticTransaction<T> implements OptimisticTransaction<T> {
    @Override
    public OptimisticTransaction<T> initialLog(Predicate<FlintMetadataLogEntry> initialCondition) {
        return this;
    }

    @Override
    public OptimisticTransaction<T> transientLog(Function<FlintMetadataLogEntry, FlintMetadataLogEntry> action) {
        return this;
    }

    @Override
    public OptimisticTransaction<T> finalLog(Function<FlintMetadataLogEntry, FlintMetadataLogEntry> action) {
        return this;
    }

    @Override
    public T commit(Function<FlintMetadataLogEntry, T> operation) {
        return operation.apply(null);
    }
}
