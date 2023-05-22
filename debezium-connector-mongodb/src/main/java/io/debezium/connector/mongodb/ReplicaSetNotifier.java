/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;

@FunctionalInterface
interface ReplicaSetNotifier<T> {

    void apply(IncrementalSnapshotContext<T> a, MongoDbPartition b, OffsetContext c);

}