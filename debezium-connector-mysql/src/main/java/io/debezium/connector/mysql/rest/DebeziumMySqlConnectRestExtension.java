/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.rest;

import java.util.Map;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.rest.ConnectRestExtension;
import org.apache.kafka.connect.rest.ConnectRestExtensionContext;

/**
 * A Kafka Connect REST extension that enables some advanced features over
 * Kafka Connect's REST interface.
 *
 * To install this extension put the jar file into a separate Kafka Connect
 * plugin dir and configure your Kafka Connect properties file with:
 *
 * `rest.extension.classes=io.debezium.connector.mysql.rest.DebeziumMySqlConnectRestExtension`
 *
 */
public class DebeziumMySqlConnectRestExtension implements ConnectRestExtension {

    private Map<String, ?> config;

    @Override
    public void register(ConnectRestExtensionContext restPluginContext) {
        restPluginContext.configurable().register(new DebeziumMySqlConnectorResource(restPluginContext.clusterState()));
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
        this.config = configs;
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }
}
