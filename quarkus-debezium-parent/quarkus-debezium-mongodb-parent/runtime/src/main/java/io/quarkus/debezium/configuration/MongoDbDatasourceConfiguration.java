/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.configuration;

import java.util.Map;

import io.debezium.runtime.configuration.QuarkusDatasourceConfiguration;

public class MongoDbDatasourceConfiguration implements QuarkusDatasourceConfiguration {

    private final String connection;
    private final String name;
    private final boolean isDefault;

    public MongoDbDatasourceConfiguration(String connection,
                                          String name,
                                          boolean isDefault) {
        this.connection = connection;
        this.isDefault = isDefault;
        this.name = name;
    }

    @Override
    public Map<String, String> asDebezium() {
        return Map.of("mongodb.connection.string", connection);
    }

    @Override
    public boolean isDefault() {
        return isDefault;
    }

    @Override
    public String getSanitizedName() {
        return name.replaceAll("[<>]", "");
    }
}
