/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.spi;

/**
 * A class invoked by {@link EventDispatcher} whenever an event is available for processing.
 *
 * @author Jiri Pechanec
 *
 */
public interface ArrivedEventListener {

    void onEvent();

    static ArrivedEventListener NO_OP = () -> {};
}
