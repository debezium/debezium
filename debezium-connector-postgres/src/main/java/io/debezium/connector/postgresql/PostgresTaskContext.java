/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.sql.SQLException;
import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.relational.TableId;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;
import io.debezium.util.ElapsedTimeStrategy;

/**
 * The context of a {@link PostgresConnectorTask}. This deals with most of the brunt of reading various configuration options
 * and creating other objects with these various options.
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
@ThreadSafe
public class PostgresTaskContext extends CdcSourceTaskContext {

    protected final static Logger LOGGER = LoggerFactory.getLogger(PostgresTaskContext.class);

    private final PostgresConnectorConfig config;
    private final TopicNamingStrategy<TableId> topicNamingStrategy;
    private final PostgresSchema schema;

    private ElapsedTimeStrategy refreshXmin;
    private Long lastXmin;

    protected PostgresTaskContext(PostgresConnectorConfig config, PostgresSchema schema, TopicNamingStrategy<TableId> topicNamingStrategy) {
        super(config.getContextName(), config.getLogicalName(), Collections::emptySet);

        this.config = config;
        if (config.xminFetchInterval().toMillis() > 0) {
            this.refreshXmin = ElapsedTimeStrategy.constant(Clock.SYSTEM, config.xminFetchInterval().toMillis());
        }
        this.topicNamingStrategy = topicNamingStrategy;
        assert schema != null;
        this.schema = schema;
    }

    protected TopicNamingStrategy<TableId> topicNamingStrategy() {
        return topicNamingStrategy;
    }

    protected PostgresSchema schema() {
        return schema;
    }

    protected PostgresConnectorConfig config() {
        return config;
    }

    protected void refreshSchema(PostgresConnection connection, boolean printReplicaIdentityInfo) throws SQLException {
        schema.refresh(connection, printReplicaIdentityInfo);
    }

    Long getSlotXmin(PostgresConnection connection) throws SQLException {
        // when xmin fetch is set to 0, we don't track it to ignore any performance of querying the
        // slot periodically
        if (config.xminFetchInterval().toMillis() <= 0) {
            return null;
        }
        assert (this.refreshXmin != null);

        if (this.refreshXmin.hasElapsed()) {
            lastXmin = getCurrentSlotState(connection).slotCatalogXmin();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Fetched new xmin from slot of {}", lastXmin);
            }
        }
        else {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("reusing xmin value of {}", lastXmin);
            }
        }

        return lastXmin;
    }

    private SlotState getCurrentSlotState(PostgresConnection connection) throws SQLException {
        return connection.getReplicationSlotState(config.slotName(), config.plugin().getPostgresPluginName());
    }

    protected ReplicationConnection createReplicationConnection(PostgresConnection jdbcConnection) throws SQLException {
        final boolean dropSlotOnStop = config.dropSlotOnStop();
        if (dropSlotOnStop) {
            LOGGER.warn(
                    "Connector has enabled automated replication slot removal upon restart ({} = true). " +
                            "This setting is not recommended for production environments, as a new replication slot " +
                            "will be created after a connector restart, resulting in missed data change events.",
                    PostgresConnectorConfig.DROP_SLOT_ON_STOP.name());
        }
        return ReplicationConnection.builder(config)
                .withSlot(config.slotName())
                .withPublication(config.publicationName())
                .withTableFilter(config.getTableFilters())
                .withPublicationAutocreateMode(config.publicationAutocreateMode())
                .withPlugin(config.plugin())
                .dropSlotOnClose(dropSlotOnStop)
                .streamParams(config.streamParams())
                .statusUpdateInterval(config.statusUpdateInterval())
                .withTypeRegistry(jdbcConnection.getTypeRegistry())
                .withSchema(schema)
                .jdbcMetadataConnection(jdbcConnection)
                .build();
    }

    PostgresConnectorConfig getConfig() {
        return config;
    }
}
