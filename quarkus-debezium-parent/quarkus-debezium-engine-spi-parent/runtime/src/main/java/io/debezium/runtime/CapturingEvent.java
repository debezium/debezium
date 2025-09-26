/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.runtime;

import java.util.Collections;
import java.util.List;

import io.debezium.common.annotation.Incubating;
import io.debezium.engine.Header;

/**
 * A capturing event with value, headers and information related to source and destination.
 *
 * @param <V>
 */
@Incubating
public sealed interface CapturingEvent<V> {

    V record();

    default <H> List<Header<H>> headers() {
        return Collections.emptyList();
    }

    /**
     * @return logical destination for which the event is intended
     */
    String destination();

    /**
     *
     * @return logical source for which the event is intended
     */
    String source();

    /***
     * @return engine for which the event is emitted
     */
    String engine();

    record Read<V>(V record, String destination, String source,
            List<Header<Object>> headers, String engine) implements CapturingEvent<V> {

    }

    record Create<V>(V record, String destination, String source,
            List<Header<Object>> headers, String engine) implements CapturingEvent<V> {

    }

    record Update<V>(V record, String destination, String source,
            List<Header<Object>> headers, String engine) implements CapturingEvent<V> {

    }

    record Delete<V>(V record, String destination, String source,
            List<Header<Object>> headers, String engine) implements CapturingEvent<V> {

    }

    record Truncate<V>(V record, String destination, String source,
            List<Header<Object>> headers, String engine) implements CapturingEvent<V> {

    }

    record Message<V>(V record, String destination, String source,
            List<Header<Object>> headers, String engine) implements CapturingEvent<V> {

    }
}
