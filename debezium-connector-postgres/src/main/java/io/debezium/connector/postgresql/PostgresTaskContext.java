/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.sql.SQLException;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;

/**
 * The context of a {@link PostgresConnectorTask}. This deals with most of the brunt of reading various configuration options
 * and creating other objects with these various options.
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
@ThreadSafe
public class PostgresTaskContext {

    private final PostgresConnectorConfig config;
    private final Clock clock;
    private final TopicSelector topicSelector;
    private final PostgresSchema schema;

    protected PostgresTaskContext(PostgresConnectorConfig config, PostgresSchema schema, TopicSelector topicSelector) {
        this.config = config;
        this.clock = Clock.system();
        this.topicSelector = topicSelector;
        assert schema != null;
        this.schema = schema;
    }

    protected Clock clock() {
        return clock;
    }

    protected TopicSelector topicSelector() {
        return topicSelector;
    }

    protected PostgresSchema schema() {
        return schema;
    }

    protected PostgresConnectorConfig config() {
        return config;
    }

    protected void refreshSchema(boolean printReplicaIdentityInfo) throws SQLException {
        try (final PostgresConnection connection = createConnection()) {
            schema.refresh(connection, printReplicaIdentityInfo);
        }
    }

    protected ReplicationConnection createReplicationConnection() throws SQLException {
        return ReplicationConnection.builder(config.jdbcConfig())
                                    .withSlot(config.slotName())
                                    .withPlugin(config.plugin())
                                    .dropSlotOnClose(config.dropSlotOnStop())
                                    .statusUpdateIntervalMillis(config.statusUpdateIntervalMillis())
                                    .withTypeRegistry(schema.getTypeRegistry())
                                    .build();
    }

    protected PostgresConnection createConnection() {
        return new PostgresConnection(config.jdbcConfig());
    }

    /**
     * Configure the logger's Mapped Diagnostic Context (MDC) properties for the thread making this call.
     *
     * @param contextName the name of the context; may not be null
     * @return the previous MDC context; never null
     * @throws IllegalArgumentException if {@code contextName} is null
     */
    protected LoggingContext.PreviousContext configureLoggingContext(String contextName) {
        return LoggingContext.forConnector("Postgres", config.serverName(), contextName);
    }

    PostgresConnectorConfig getConfig() {
        return config;
    }
}
