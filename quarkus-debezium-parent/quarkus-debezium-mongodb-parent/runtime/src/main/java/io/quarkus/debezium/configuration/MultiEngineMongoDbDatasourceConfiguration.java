/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.configuration;

import java.util.Map;

import io.debezium.runtime.configuration.QuarkusDatasourceConfiguration;

public class MultiEngineMongoDbDatasourceConfiguration implements QuarkusDatasourceConfiguration {

    private final Map<String, MongoDbDatasourceConfiguration> configurations;

    public MultiEngineMongoDbDatasourceConfiguration(Map<String, MongoDbDatasourceConfiguration> configurations) {
        this.configurations = configurations;
    }

    @Override
    public Map<String, String> asDebezium() {
        return configurations.get(QuarkusDatasourceConfiguration.DEFAULT).asDebezium();
    }

    @Override
    public boolean isDefault() {
        return false;
    }

    @Override
    public String getSanitizedName() {
        return configurations.get(DEFAULT).getSanitizedName();
    }

    public Map<String, MongoDbDatasourceConfiguration> get() {
        return configurations;
    }
}
