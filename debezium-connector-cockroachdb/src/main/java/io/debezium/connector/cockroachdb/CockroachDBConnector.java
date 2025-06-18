/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.cockroachdb;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.ExactlyOnceSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.common.BaseSourceConnector;

/**
 * Kafka Connect source connector for CockroachDB.
 * Uses CockroachDB's native changefeed (enriched envelope) as the source of truth.
 *
 * * @author Virag Tripathi
 */
public class CockroachDBConnector extends BaseSourceConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBConnector.class);

    private Map<String, String> configProps;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return CockroachDBConnectorTask.class;
    }

    @Override
    public void start(Map<String, String> props) {
        this.configProps = new HashMap<>(props);
        LOGGER.info("Starting CockroachDB Connector with config: {}", props);
    }

    @Override
    public void stop() {
        LOGGER.info("Stopping CockroachDB Connector");
        this.configProps = null;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return Collections.singletonList(configProps);
    }

    @Override
    public ExactlyOnceSupport exactlyOnceSupport(Map<String, String> connectorConfig) {
        return ExactlyOnceSupport.UNSUPPORTED;
    }

    @Override
    protected void validateConnectorConfiguration(Configuration config) {
        // TODO: Add proper field-level validation logic here
    }
}
