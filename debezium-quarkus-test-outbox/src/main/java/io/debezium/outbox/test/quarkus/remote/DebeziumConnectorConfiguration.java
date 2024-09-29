/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.test.quarkus.remote;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

public record DebeziumConnectorConfiguration(@JsonProperty("name") String name,
                                             @JsonProperty("config") DebeziumConnectorConfigurationConfig config) {

    public DebeziumConnectorConfiguration(final Builder builder) {
        this(builder.name, builder.config);
    }

    public DebeziumConnectorConfiguration {
        Objects.requireNonNull(name);
        Objects.requireNonNull(config);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private String name;
        private DebeziumConnectorConfigurationConfig config;

        public Builder withName(final String name) {
            this.name = name;
            return this;
        }

        public Builder withConfig(final DebeziumConnectorConfigurationConfig config) {
            this.config = config;
            return this;
        }

        public DebeziumConnectorConfiguration build() {
            return new DebeziumConnectorConfiguration(this);
        }
    }

}
