/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver.junit;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import io.debezium.connector.sqlserver.SqlServerConnection;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.junit.DatabaseVersionResolver;

public class SqlServerDatabaseVersionResolver implements DatabaseVersionResolver {
    @Override
    public DatabaseVersion getVersion() {
        try (SqlServerConnection connection = TestHelper.adminConnection()) {
            final DatabaseMetaData metadata = connection.connection().getMetaData();
            return new DatabaseVersion(metadata.getDatabaseMajorVersion(), metadata.getDatabaseMinorVersion(), 0);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to resolve SQL Server database version", e);
        }
    }
}
