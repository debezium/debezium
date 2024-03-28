/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.util;

import java.util.Map;
import java.util.Random;

import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.jdbc.JdbcConnection;

/**
 * An implementation of {@link UniqueDatabase} for MariaDB.
 *
 * @author Chris Cranford
 */
public class MariaDbUniqueDatabase extends UniqueDatabase {

    public MariaDbUniqueDatabase(String serverName, String databaseName) {
        this(serverName, databaseName, Integer.toUnsignedString(new Random().nextInt(), 36), null);
    }

    public MariaDbUniqueDatabase(String serverName, String databaseName, String identifier, String charSet) {
        super(serverName, databaseName, identifier, charSet);
    }

    @Override
    protected JdbcConnection forTestDatabase(String databaseName, Map<String, Object> urlProperties) {
        return MariaDbTestConnection.forTestDatabase(databaseName, urlProperties);
    }

}
