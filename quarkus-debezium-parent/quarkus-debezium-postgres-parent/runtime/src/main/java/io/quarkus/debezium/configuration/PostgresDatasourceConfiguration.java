/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.configuration;

import java.util.Map;

import io.debezium.runtime.configuration.QuarkusDatasourceConfiguration;

public class PostgresDatasourceConfiguration implements QuarkusDatasourceConfiguration {
    private final String host;
    private final String username;
    private final String password;
    private final String database;
    private final String port;
    private final boolean isDefault;
    private final String name;

    public PostgresDatasourceConfiguration(String host,
                                           String username,
                                           String password,
                                           String database,
                                           String port,
                                           boolean isDefault,
                                           String name) {
        this.host = host;
        this.username = username;
        this.password = password;
        this.database = database;
        this.port = port;
        this.isDefault = isDefault;
        this.name = name;
    }

    @Override
    public Map<String, String> asDebezium() {
        return Map.of(
                "name", name.replaceAll("[<>]", ""),
                "database.hostname", host,
                "database.port", port,
                "database.user", username,
                "database.password", password,
                "database.dbname", database);
    }

    @Override
    public boolean isDefault() {
        return isDefault;
    }

    public String getName() {
        return name;
    }
}
