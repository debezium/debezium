/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.reactive;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.embedded.spi.OffsetCommitPolicy;
import io.debezium.reactive.internal.ReactiveEngineBuilderImpl;
import io.debezium.reactive.spi.AsType;
import io.debezium.util.Clock;
import io.reactivex.Flowable;

/**
 * An embedded engine that uses Rx Java 2 as an API. Internally this class encapsulates {@link EmbeddedEngine}
 * and exposes the change events coming from database as an RX stream.<br/>
 * 
 * The events itself are encapsulated in {@link DataChangeEvent} class.
 * The consumer is expected to extract the Debezium generated {@link SourceRecord} or it its serialized form and when the record
 * is durably processed then the consumer must commit the record so the record offset is stored in offset store.<br/>
 * 
 * This guarantees that no record is lost in case of failure but the consumer still must be prepared to handle
 * duplicates if the failure happens between durable processing and offset write.</br>
 *
 * The class itself is created using {@link Builder} that is created using {@link #create()} method.
 *
 * @author Jiri Pechanec
 */
public interface ReactiveEngine<T> {

    /**
     * A builder to set up and create {@link ReactiveEngine} instances.
     */
    public static interface Builder<T> {

        Builder<T> withConfiguration(Configuration config);

        Builder<T> withClassLoader(ClassLoader classLoader);

        Builder<T> withClock(Clock clock);

        Builder<T> withOffsetCommitPolicy(OffsetCommitPolicy policy);

        Builder<T> asType(Class<? extends AsType<T>> asType);

        ReactiveEngine<T> build();
    }

    /**
     * @return default implementation of reactive engine Builder
     */
    public static <T> Builder<T> create() {
        return new ReactiveEngineBuilderImpl<T>();
    }

    /**
     * @return a stream of change events from Debezium connector
     */
    Flowable<DataChangeEvent<T>> stream();
}
