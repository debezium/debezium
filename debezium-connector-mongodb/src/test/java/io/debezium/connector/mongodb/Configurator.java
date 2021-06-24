/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.util.Testing;

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
        if (Math.random() >= 0.5) {
            Testing.debug("Using \"" + MongoDbConnectorConfig.DATABASE_WHITELIST.name() + "\" config property");
            return with(MongoDbConnectorConfig.DATABASE_WHITELIST, regexList);
        }
        Testing.debug("Using \"" + MongoDbConnectorConfig.DATABASE_INCLUDE_LIST.name() + "\" config property");
        return with(MongoDbConnectorConfig.DATABASE_INCLUDE_LIST, regexList);
    }

    public Configurator excludeDatabases(String regexList) {
        if (Math.random() >= 0.5) {
            Testing.debug("Using \"" + MongoDbConnectorConfig.DATABASE_BLACKLIST.name() + "\" config property");
            return with(MongoDbConnectorConfig.DATABASE_BLACKLIST, regexList);
        }
        Testing.debug("Using \"" + MongoDbConnectorConfig.DATABASE_EXCLUDE_LIST.name() + "\" config property");
        return with(MongoDbConnectorConfig.DATABASE_EXCLUDE_LIST, regexList);
    }

    public Configurator includeCollections(String regexList) {
        if (Math.random() >= 0.5) {
            Testing.debug("Using \"" + MongoDbConnectorConfig.COLLECTION_WHITELIST.name() + "\" config property");
            return with(MongoDbConnectorConfig.COLLECTION_WHITELIST, regexList);
        }
        Testing.debug("Using \"" + MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST.name() + "\" config property");
        return with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, regexList);
    }

    public Configurator excludeCollections(String regexList) {
        if (Math.random() >= 0.5) {
            Testing.debug("Using \"" + MongoDbConnectorConfig.COLLECTION_BLACKLIST.name() + "\" config property");
            return with(MongoDbConnectorConfig.COLLECTION_BLACKLIST, regexList);
        }
        Testing.debug("Using \"" + MongoDbConnectorConfig.COLLECTION_EXCLUDE_LIST.name() + "\" config property");
        return with(MongoDbConnectorConfig.COLLECTION_EXCLUDE_LIST, regexList);
    }

    public Configurator excludeFields(String excludeList) {
        if (Math.random() >= 0.5) {
            Testing.debug("Using \"" + MongoDbConnectorConfig.FIELD_BLACKLIST.name() + "\" config property");
            return with(MongoDbConnectorConfig.FIELD_BLACKLIST, excludeList);
        }
        Testing.debug("Using \"" + MongoDbConnectorConfig.FIELD_EXCLUDE_LIST.name() + "\" config property");
        return with(MongoDbConnectorConfig.FIELD_EXCLUDE_LIST, excludeList);
    }

    public Configurator renameFields(String renames) {
        return with(MongoDbConnectorConfig.FIELD_RENAMES, renames);
    }

    public Filters createFilters() {
        return new Filters(configBuilder.build());
    }

}
