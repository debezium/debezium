/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.capture;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.engine.RecordChangeEvent;
import io.debezium.runtime.Capturing;

/**
 *
 * Invoker assigned to any annotated class with method {@link Capturing}
 *
 */
public interface CapturingInvoker {

    /**
     * The event that is captured by Debezium
     * @param event
     */
    void capture(RecordChangeEvent<SourceRecord> event);

    /**
     * Return the assigned fully qualified table name
     * @return String
     */
    String getFullyQualifiedTableName();
}
