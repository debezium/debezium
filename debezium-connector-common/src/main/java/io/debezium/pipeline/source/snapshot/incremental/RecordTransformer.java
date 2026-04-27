/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import org.apache.kafka.connect.source.SourceRecord;

/**
 * Transforms a SourceRecord before delivery to snapshot completion handlers.
 *
 * <p>Implementations typically wrap the engine's configured SMT chain so that
 * snapshot events receive the same transformations as CDC events.
 *
 * <p>If no transformer is registered, the worker passes records through unchanged.
 *
 * @author Ivan Senyk
 */
@FunctionalInterface
public interface RecordTransformer {
    SourceRecord transform(SourceRecord record);
}
