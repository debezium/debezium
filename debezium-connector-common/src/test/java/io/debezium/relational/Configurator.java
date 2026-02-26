/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.util.Testing;

/**
 * A helper for easily building connector configurations for testing.
 *
 * @author ggaborg
 */
public class Configurator {

    private Configuration.Builder configBuilder = Configuration.create();

    private Configurator with(Field field, String value) {
        configBuilder.with(field, value);
        return this;
    }

    public Configurator includeDatabases(String regexList) {
        Testing.debug("Using \"" + RelationalDatabaseConnectorConfig.DATABASE_INCLUDE_LIST.name() + "\" config property");
        return with(RelationalDatabaseConnectorConfig.DATABASE_INCLUDE_LIST, regexList);
    }

    public Configurator excludeDatabases(String regexList) {
        Testing.debug("Using \"" + RelationalDatabaseConnectorConfig.DATABASE_EXCLUDE_LIST.name() + "\" config property");
        return with(RelationalDatabaseConnectorConfig.DATABASE_EXCLUDE_LIST, regexList);
    }

    public Configurator includeCollections(String regexList) {
        Testing.debug("Using \"" + RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST.name() + "\" config property");
        return with(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST, regexList);
    }

    public Configurator excludeCollections(String regexList) {
        Testing.debug("Using \"" + RelationalDatabaseConnectorConfig.TABLE_EXCLUDE_LIST.name() + "\" config property");
        return with(RelationalDatabaseConnectorConfig.TABLE_EXCLUDE_LIST, regexList);
    }

    public Configurator storeOnlyCapturedDatabasesDdl(String flag) {
        Testing.debug("Using \"" + HistorizedRelationalDatabaseConnectorConfig.STORE_ONLY_CAPTURED_DATABASES_DDL.name() + "\" config property");
        return with(HistorizedRelationalDatabaseConnectorConfig.STORE_ONLY_CAPTURED_DATABASES_DDL, flag);
    }

    public Configurator signalingCollection(String signalingCollection) {
        return with(RelationalDatabaseConnectorConfig.SIGNAL_DATA_COLLECTION, signalingCollection);
    }

    public RelationalTableFilters createFilters() {
        return new RelationalTableFilters(configBuilder.build(), t -> true, TableId::toString, false);
    }

}
