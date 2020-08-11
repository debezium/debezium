/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.heartbeat;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.function.BlockingConsumer;
import io.debezium.jdbc.JdbcConnection;

/**
 *  Implementation of the heartbeat feature that allows for a DB query to be executed with every heartbeat.
 */
public class DatabaseHeartbeatImpl extends HeartbeatImpl {
    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseHeartbeatImpl.class);

    public static final String HEARTBEAT_ACTION_QUERY_PROPERTY_NAME = "heartbeat.action.query";

    public static final Field HEARTBEAT_ACTION_QUERY = Field.create(HEARTBEAT_ACTION_QUERY_PROPERTY_NAME)
            .withDisplayName("An optional query to execute with every heartbeat")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("The query executed with every heartbeat.");

    private final String heartBeatActionQuery;
    private final JdbcConnection jdbcConnection;

    DatabaseHeartbeatImpl(Configuration configuration, String topicName, String key, JdbcConnection jdbcConnection, String heartBeatActionQuery) {
        super(configuration, topicName, key);

        this.heartBeatActionQuery = heartBeatActionQuery;
        this.jdbcConnection = jdbcConnection;
    }

    @Override
    public void forcedBeat(Map<String, ?> partition, Map<String, ?> offset, BlockingConsumer<SourceRecord> consumer) throws InterruptedException {
        try {
            jdbcConnection.execute(heartBeatActionQuery);
        }
        catch (Exception e) {
            LOGGER.error("Could not execute heartbeat action", e);
        }
        LOGGER.debug("Executed heartbeat action query");

        super.forcedBeat(partition, offset, consumer);
    }
}
