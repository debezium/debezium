/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.rest;

import java.util.Map;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.rest.ConnectRestExtension;
import org.apache.kafka.connect.rest.ConnectRestExtensionContext;

/**
 *
 * @author Chris Cranford
 */
public class DebeziumMariaDbConnectRestExtension implements ConnectRestExtension {
    private Map<String, ?> config;

    @Override
    public void register(ConnectRestExtensionContext restPluginContext) {
        restPluginContext.configurable().register(new DebeziumMariaDbConnectorResource(restPluginContext.clusterState()));
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
