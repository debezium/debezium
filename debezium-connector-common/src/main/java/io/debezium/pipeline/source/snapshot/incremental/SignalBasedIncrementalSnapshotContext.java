/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.util.Map;

import io.debezium.annotation.NotThreadSafe;

/**
 * A class describing current state of incremental snapshot
 *
 * @author Jiri Pechanec
 *
 */
@NotThreadSafe
public class SignalBasedIncrementalSnapshotContext<T> extends AbstractIncrementalSnapshotContext<T> {

    public SignalBasedIncrementalSnapshotContext() {
        this(true);
    }

    public SignalBasedIncrementalSnapshotContext(boolean useCatalogBeforeSchema) {
        super(useCatalogBeforeSchema);
    }

    public static <U> IncrementalSnapshotContext<U> load(Map<String, ?> offsets) {
        return load(offsets, true);
    }

    public static <U> SignalBasedIncrementalSnapshotContext<U> load(Map<String, ?> offsets, boolean useCatalogBeforeSchema) {
        final SignalBasedIncrementalSnapshotContext<U> context = new SignalBasedIncrementalSnapshotContext<>(useCatalogBeforeSchema);
        init(context, offsets);
        return context;
    }

}
