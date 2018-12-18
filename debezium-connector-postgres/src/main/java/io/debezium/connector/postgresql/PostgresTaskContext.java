/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.sql.SQLException;
import java.util.Collections;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.connection.ServerInfo;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The context of a {@link PostgresConnectorTask}. This deals with most of the brunt of reading various configuration options
 * and creating other objects with these various options.
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
@ThreadSafe
public class PostgresTaskContext extends CdcSourceTaskContext {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final PostgresConnectorConfig config;
    private final TopicSelector<TableId> topicSelector;
    private final PostgresSchema schema;

    private Long lastXminTime;
    private Long lastXmin;

    protected PostgresTaskContext(PostgresConnectorConfig config, PostgresSchema schema, TopicSelector<TableId> topicSelector) {
        super("Postgres", config.getLogicalName(), Collections::emptySet);

        this.config = config;
        this.topicSelector = topicSelector;
        assert schema != null;
        this.schema = schema;
    }

    protected TopicSelector<TableId> topicSelector() {
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

    protected Long getSlotXmin() throws SQLException {
        if (!config.writeRecoverySnapshot()) {
            return null;
        }

        if (lastXminTime == null || getClock().currentTimeInMillis() >= lastXminTime + config.xminFetchInterval()) {
            lastXmin = getCurrentSlotInfo().catalogXmin();
            lastXminTime = getClock().currentTimeInMillis();
            if (logger.isDebugEnabled()) {
                logger.debug("Fetched new xmin from slot of {}", lastXmin);
            }
        }
        else {
            if (logger.isDebugEnabled()) {
                logger.debug("reusing xmin value of {} from {}", lastXmin, lastXminTime);
            }
        }

        return lastXmin;
    }

    protected ServerInfo.ReplicationSlot getCurrentSlotInfo() throws SQLException {
        ServerInfo.ReplicationSlot slotInfo;
        try (final PostgresConnection connection = createConnection()) {
            slotInfo = connection.fetchReplicationSlotInfo(config.slotName(), config.plugin().getPostgresPluginName());
        }
        return slotInfo;
    }

    protected boolean isXminInSlot(Long xmin) throws SQLException {
        if (xmin == null) {
            return false;
        }
        Long curXmin = getCurrentSlotInfo().catalogXmin();
        if (curXmin == null) {
            curXmin = -1L;
        }
        logger.debug("last seen xmin: {}, current xmin: {}", xmin, curXmin);
        return xmin >= curXmin;
    }

    protected boolean isLsnInSlot(Long lsn) throws SQLException {
        if (lsn == null) {
            return false;
        }
        Long curLsn = getCurrentSlotInfo().latestFlushedLSN();
        if (curLsn == null) {
            curLsn = -1L;
        }
        logger.debug("last seen lsn: {}, current lsn: {}", lsn, curLsn);
        return lsn >= curLsn;
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
