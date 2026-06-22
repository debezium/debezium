/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import java.io.Closeable;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.common.annotation.Incubating;

/**
 * Abstraction for the transformation chain applied to {@link SourceRecord} instances
 * by the embedded engine. Implementations apply configured SMT (Single Message
 * Transformation) chains to records before they are passed to the consumer.
 *
 * @author Debezium Authors
 */
@Incubating
public interface SourceRecordTransformations extends Closeable {

    /**
     * Apply the configured transformation chain to the given record.
     *
     * @param record the source record to transform
     * @return the transformed record, or {@code null} if the record should be dropped
     */
    SourceRecord transform(SourceRecord record);
}
