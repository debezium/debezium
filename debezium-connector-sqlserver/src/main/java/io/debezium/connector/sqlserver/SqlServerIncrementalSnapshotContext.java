/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.util.Map;

import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.relational.TableId;

public class SqlServerIncrementalSnapshotContext<T> extends SignalBasedIncrementalSnapshotContext<T> {

    public SqlServerIncrementalSnapshotContext() {
        this(true);
    }

    public SqlServerIncrementalSnapshotContext(boolean useCatalogBeforeSchema) {
        super(useCatalogBeforeSchema);
    }

    public static <U> IncrementalSnapshotContext<U> load(Map<String, ?> offsets) {
        return load(offsets, true);
    }

    public static <U> SqlServerIncrementalSnapshotContext<U> load(Map<String, ?> offsets, boolean useCatalogBeforeSchema) {
        final SqlServerIncrementalSnapshotContext<U> context = new SqlServerIncrementalSnapshotContext<>(useCatalogBeforeSchema);
        init(context, offsets);
        return context;
    }

    @Override
    public TableId getPredicateBasedTableIdForId(TableId id) {
        return id.toBracketQuoted();
    }

    @Override
    public TableId getPredicateBasedTableIdForString(String id) {
        return TableId.parse(id, shouldUseCatalogBeforeSchema(), new SqlServerTableIdPredicates());
    }
}
