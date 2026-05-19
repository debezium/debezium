/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.ydb;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

/**
 * Shared connection settings for YDB-backed storage (offset store, schema history).
 */
public class YdbCommonConfig {

    public static final Field ENDPOINT = Field.create("endpoint")
            .withDescription("YDB endpoint, e.g. grpcs://ydb.example:2135")
            .required();

    public static final Field DATABASE = Field.create("database")
            .withDescription("YDB database path, e.g. /Root/debezium")
            .required();

    public static final Field AUTH_USER = Field.create("auth.user")
            .withDescription("YDB static credentials: login (optional; anonymous if not set)");

    public static final Field AUTH_PASSWORD = Field.create("auth.password")
            .withDescription("YDB static credentials: password");

    private final String endpoint;
    private final String database;
    private final String authUser;
    private final String authPassword;

    public YdbCommonConfig(Configuration config) {
        this.endpoint = config.getString(ENDPOINT);
        this.database = config.getString(DATABASE);
        this.authUser = config.getString(AUTH_USER);
        this.authPassword = config.getString(AUTH_PASSWORD);
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getDatabase() {
        return database;
    }

    public String getAuthUser() {
        return authUser;
    }

    public String getAuthPassword() {
        return authPassword;
    }
}