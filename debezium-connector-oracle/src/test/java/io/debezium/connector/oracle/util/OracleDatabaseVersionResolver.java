/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.util;

import java.sql.SQLException;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleDatabaseVersion;
import io.debezium.junit.DatabaseVersionResolver;

/**
 * Implementation of {@link DatabaseVersionResolver} specific for Oracle.
 *
 * @author Chris Cranford
 */
public class OracleDatabaseVersionResolver implements DatabaseVersionResolver {
    @Override
    public DatabaseVersion getVersion() {
        try (OracleConnection connection = TestHelper.testConnection()) {
            OracleDatabaseVersion version = connection.getOracleVersion();
            return new DatabaseVersion(version.getMajor(), version.getMaintenance(), version.getAppServer());
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to resolve database version", e);
        }
    }
}
