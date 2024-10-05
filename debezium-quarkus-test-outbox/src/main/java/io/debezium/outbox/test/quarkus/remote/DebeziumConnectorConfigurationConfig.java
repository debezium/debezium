/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.test.quarkus.remote;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents the configuration of the Debezium Connector (Config object).
 */
public final class DebeziumConnectorConfigurationConfig {

    final String databaseHostname;
    final Integer databasePort;
    final String databaseUser;
    final String databasePassword;
    final String databaseDbname;
    final String databaseServerName;

    /**
     * Constructor using a Builder.
     *
     * @param builder the Builder object
     */
    public DebeziumConnectorConfigurationConfig(final Builder builder) {
        this(builder.databaseHostname, builder.databasePort, builder.databaseUser, builder.databasePassword, builder.databaseDbname, builder.databaseServerName);
    }

    /**
     * Constructor for creating the DebeziumConnectorConfigurationConfig with specific parameters.
     *
     * @param databaseHostname the hostname of the database
     * @param databasePort the port of the database
     * @param databaseUser the username for the database
     * @param databasePassword the password for the database
     * @param databaseDbname the database name
     * @param databaseServerName the server name for the database
     * @throws NullPointerException if any argument is null
     */
    public DebeziumConnectorConfigurationConfig(final String databaseHostname, final Integer databasePort, final String databaseUser,
                                                final String databasePassword, final String databaseDbname,
                                                final String databaseServerName) {
        this.databaseHostname = Objects.requireNonNull(databaseHostname);
        this.databasePort = Objects.requireNonNull(databasePort);
        this.databaseUser = Objects.requireNonNull(databaseUser);
        this.databasePassword = Objects.requireNonNull(databasePassword);
        this.databaseDbname = Objects.requireNonNull(databaseDbname);
        this.databaseServerName = Objects.requireNonNull(databaseServerName);
    }

    /**
     * Creates a new Builder for DebeziumConnectorConfigurationConfig.
     *
     * @return a new instance of Builder
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder class for DebeziumConnectorConfigurationConfig.
     */
    public static class Builder {
        private String databaseHostname;
        private Integer databasePort;
        private String databaseUser;
        private String databasePassword;
        private String databaseDbname;
        private String databaseServerName;

        public Builder withDatabaseHostname(final String databaseHostname) {
            this.databaseHostname = databaseHostname;
            return this;
        }

        public Builder withDatabasePort(final Integer databasePort) {
            this.databasePort = databasePort;
            return this;
        }

        public Builder withDatabaseUser(final String databaseUser) {
            this.databaseUser = databaseUser;
            return this;
        }

        public Builder withDatabasePassword(final String databasePassword) {
            this.databasePassword = databasePassword;
            return this;
        }

        public Builder withDatabaseDbname(final String databaseDbname) {
            this.databaseDbname = databaseDbname;
            return this;
        }

        public Builder withDatabaseServerName(final String databaseServerName) {
            this.databaseServerName = databaseServerName;
            return this;
        }

        public DebeziumConnectorConfigurationConfig build() {
            return new DebeziumConnectorConfigurationConfig(this);
        }

    }

    // Various getter methods for Debezium connector properties, annotated with @JsonProperty for serialization

    @JsonProperty("connector.class")
    public String getConnectorClass() {
        return "io.debezium.connector.postgresql.PostgresConnector";
    }

    @JsonProperty("tasks.max")
    public String getTasksMax() {
        return "1";
    }

    @JsonProperty("database.hostname")
    public String getDatabaseHostname() {
        return databaseHostname;
    }

    @JsonProperty("database.port")
    public String getDatabasePort() {
        return databasePort.toString();
    }

    @JsonProperty("database.user")
    public String getDatabaseUser() {
        return databaseUser;
    }

    @JsonProperty("database.password")
    public String getDatabasePassword() {
        return databasePassword;
    }

    @JsonProperty("database.dbname")
    public String getDatabaseDbname() {
        return databaseDbname;
    }

    @JsonProperty("database.server.name")
    public String getDatabaseServerName() {
        return databaseServerName;
    }

    @JsonProperty("table.include.list")
    public String getTableIncludeList() {
        return "public.outboxevent";
    }

    @JsonProperty("topic.prefix")
    public String getTopicPrefix() {
        return "dbserver1";
    }

    // avoid schema in key part
    @JsonProperty("key.converter")
    public String getKeyConverter() {
        return "org.apache.kafka.connect.json.JsonConverter";
    }

    @JsonProperty("key.converter.schemas.enable")
    public String getKeyConverterSchemasEnable() {
        return "false";
    }
    // \avoid schema in key part

    // avoid schema in value part
    @JsonProperty("value.converter")
    public String getValueConverter() {
        return "org.apache.kafka.connect.json.JsonConverter";
    }

    @JsonProperty("value.converter.schemas.enable")
    public String getValueConverterSchemasEnable() {
        return "false";
    }
    // \avoid schema in value part

    @JsonProperty("tombstones.on.delete")
    public String getTombstonesOnDelete() {
        return "false";
    }

    // multiple transforms for extracting after and convert String to json on fields payload and materializedstate
    @JsonProperty("transforms")
    public String getTransforms() {
        return "outbox";
    }

    @JsonProperty("transforms.outbox.type")
    public String getTransformsOutboxType() {
        return "io.debezium.transforms.outbox.EventRouter";
    }

    @JsonProperty("transforms.outbox.route.topic.replacement")
    public String getTransformsOutboxRouteTopicReplacement() {
        return "${routedByValue}.events";
    }

    @JsonProperty("transforms.outbox.table.fields.additional.placement")
    public String getTransformsOutboxTableFieldsAdditionalPlacement() {
        return "type:header:eventType";
    }
}
