/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.reactive;

import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.embedded.spi.OffsetCommitPolicy;
import io.debezium.reactive.internal.ReactiveEngineBuilderImpl;
import io.debezium.util.Clock;
import io.reactivex.Flowable;

/**
 * An embedded engine that uses Rx Java 2 as an API. Internally this class encapsulates {@link EmbeddedEngine}
 * and exposes the change events coming from database as an RX stream.<br/>
 * 
 * The events itself are encapsulated in {@link DebeziumEvent} class.
 * The consumer is expected to extract the Debezium generated {@link SourceRecord} and when the record
 * is durably processed then the consumer must commit the record so the record offset is stored in offset store.<br/>
 * 
 * This guarantees that no record is lost in case of failure but the consumer still must be prepared to handle
 * duplicates if the failure happens between durable processing and offset write.</br>
 *
 * The class itself is created using {@link Builder} that is created using {@link #create()} method.
 *
 * @author Jiri Pechanec
 */
public interface ReactiveEngine {

    /**
     * A builder to set up and create {@link ReactiveEngine} instances.
     */
    public static interface Builder {

        Builder using(Configuration config);

        Builder using(ClassLoader classLoader);

        Builder using(Clock clock);

        Builder using(OffsetCommitPolicy policy);

        ReactiveEngine build();
    }

    /**
     * @return Default implementation of reactive engine Builder
     */
    public static Builder create() {
        return new ReactiveEngineBuilderImpl();
    }

    /**
     * @return a stream of change events from Debezium connector
     */
    Flowable<DebeziumEvent> flowableWithBackPressure();
}
