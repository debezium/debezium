/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.capture.consumer;

import java.util.function.Consumer;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;

/**
 * Consumer for the {@link DebeziumEngine} notifying(Consumer<R> consumer)
 * used for a Quarkus {@link SourceRecord} Debezium engine instance
 */
public interface SourceRecordEventConsumer extends Consumer<ChangeEvent<SourceRecord, SourceRecord>> {
    @Override
    void accept(ChangeEvent<SourceRecord, SourceRecord> sourceRecordSourceRecordChangeEvent);
}
