/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

/**
 * A helper for easily building connector configurations for testing.
 *
 * @author Randall Hauch
 */
public class Configurator {

    private Configuration.Builder configBuilder = Configuration.create();

    public Configurator with(Field field, String value) {
        configBuilder.with(field, value);
        return this;
    }

    public Configurator with(Field field, boolean value) {
        configBuilder.with(field, value);
        return this;
    }

    public Configurator with(Field field, int value) {
        configBuilder.with(field, value);
        return this;
    }

    public Configurator serverName(String serverName) {
        return with(MongoDbConnectorConfig.LOGICAL_NAME, serverName);
    }

    public Configurator hosts(String hosts) {
        return with(MongoDbConnectorConfig.HOSTS, hosts);
    }

    public Configurator maxBatchSize(int maxBatchSize) {
        return with(MongoDbConnectorConfig.MAX_BATCH_SIZE, maxBatchSize);
    }

    public Configurator includeDatabases(String regexList) {
        return with(MongoDbConnectorConfig.DATABASE_WHITELIST, regexList);
    }

    public Configurator excludeDatabases(String regexList) {
        return with(MongoDbConnectorConfig.DATABASE_BLACKLIST, regexList);
    }

    public Configurator includeCollections(String regexList) {
        return with(MongoDbConnectorConfig.COLLECTION_WHITELIST, regexList);
    }

    public Configurator excludeCollections(String regexList) {
        return with(MongoDbConnectorConfig.COLLECTION_BLACKLIST, regexList);
    }

    public Filters createFilters() {
        return new Filters(configBuilder.build());
    }

}