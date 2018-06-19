/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.sql.SQLException;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;

/**
 * The context of a {@link PostgresConnectorTask}. This deals with most of the brunt of reading various configuration options
 * and creating other objects with these various options.
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
@ThreadSafe
public class PostgresTaskContext extends CdcSourceTaskContext {

    private final PostgresConnectorConfig config;
    private final TopicSelector topicSelector;
    private final PostgresSchema schema;

    protected PostgresTaskContext(PostgresConnectorConfig config, PostgresSchema schema, TopicSelector topicSelector) {
        super("Postgres", config.serverName());

        this.config = config;
        this.topicSelector = topicSelector;
        assert schema != null;
        this.schema = schema;
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

    PostgresConnectorConfig getConfig() {
        return config;
    }
}
