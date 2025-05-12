/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.runtime;

import io.debezium.common.annotation.Incubating;

@Incubating
public record DebeziumManifest(
        Connector connector,
        Status status) {
    public record Status(State state) {
        public enum State {
            CREATING,
            POLLING,
            STOPPED
        }
    }

    public record Connector(String name) {
    }
}
