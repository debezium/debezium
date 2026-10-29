/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.spi;

import java.util.Map;

/**
 * Invoked whenever an important event or change of state happens during the streaming phase.
 */
public interface StreamingProgressListener {

    void connected(boolean connected);

    /**
     * Invoked when the streaming connection is established, with the identity of the server that provides it, as read on
     * that connection.
     *
     * @param server the connector-specific identity of the server; may be empty but not null
     */
    default void serverConnected(Map<String, String> server) {
    }

    /**
     * Invoked when the streaming connection is lost.
     */
    default void serverDisconnected() {
    }
}
