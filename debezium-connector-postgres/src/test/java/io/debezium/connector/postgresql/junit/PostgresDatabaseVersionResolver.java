/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.junit;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import io.debezium.connector.postgresql.TestHelper;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.junit.DatabaseVersionResolver;

/**
 * Implementation of {@link DatabaseVersionResolver} specific for PostgreSQL.
 *
 * @author Chris Cranford
 */
public class PostgresDatabaseVersionResolver implements DatabaseVersionResolver {
    @Override
    public DatabaseVersion getVersion() {
        try {
            try (PostgresConnection postgresConnection = TestHelper.create()) {
                final DatabaseMetaData metadata = postgresConnection.connection().getMetaData();
                return new DatabaseVersion(metadata.getDatabaseMajorVersion(), metadata.getDatabaseMinorVersion(), 0);
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
