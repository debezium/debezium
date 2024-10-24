/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.test.quarkus.remote;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents the configuration of a Debezium Connector.
 */
public record DebeziumConnectorConfiguration(@JsonProperty("name") String name,
                                             @JsonProperty("config") DebeziumConnectorConfigurationConfig config) {

    /**
     * Constructor using a Builder.
     *
     * @param builder the Builder object to build the configuration
     */
    public DebeziumConnectorConfiguration(final Builder builder) {
        this(builder.name, builder.config);
    }

    /**
     * Constructor that requires name and config to be non-null.
     *
     * @param name the name of the Debezium connector
     * @param config the configuration of the Debezium connector
     * @throws NullPointerException if name or config is null
     */
    public DebeziumConnectorConfiguration {
        Objects.requireNonNull(name);
        Objects.requireNonNull(config);
    }

    /**
     * Creates a new Builder for DebeziumConnectorConfiguration.
     *
     * @return a new instance of Builder
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder class for DebeziumConnectorConfiguration.
     */
    public static class Builder {
        private String name;
        private DebeziumConnectorConfigurationConfig config;

        /**
         * Sets the name of the connector.
         *
         * @param name the name of the connector
         * @return the updated Builder instance
         */
        public Builder withName(final String name) {
            this.name = name;
            return this;
        }

        /**
         * Sets the configuration of the connector.
         *
         * @param config the configuration object
         * @return the updated Builder instance
         */
        public Builder withConfig(final DebeziumConnectorConfigurationConfig config) {
            this.config = config;
            return this;
        }

        /**
         * Builds the DebeziumConnectorConfiguration.
         *
         * @return a new instance of DebeziumConnectorConfiguration
         */
        public DebeziumConnectorConfiguration build() {
            return new DebeziumConnectorConfiguration(this);
        }
    }

}
