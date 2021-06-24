/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.junit;

import io.debezium.connector.mysql.MySqlTestConnection;
import io.debezium.junit.DatabaseVersionResolver;

/**
 * Implementation of {@link DatabaseVersionResolver} specific for MySQL.
 *
 * @author Chris Cranford
 */
public class MySqlDatabaseVersionResolver implements DatabaseVersionResolver {

    public DatabaseVersion getVersion() {
        final String versionString = MySqlTestConnection.forTestDatabase("mysql").getMySqlVersionString();

        final String[] tokens = versionString.split("\\.");
        if (tokens.length == 0) {
            throw new IllegalStateException("Failed to resolve database version");
        }

        int major = sanitizeAndParseToken(tokens[0]);
        int minor = tokens.length >= 2 ? sanitizeAndParseToken(tokens[1]) : 0;
        int patch = tokens.length >= 3 ? sanitizeAndParseToken(tokens[2]) : 0;

        return new DatabaseVersion(major, minor, patch);
    }

    private static int sanitizeAndParseToken(String token) {
        // Sometimes the MySQL version string tokens contain non-numeric content, such as '5.5.62-log'.
        // In these cases, this method will adequately parse each sub-token such that the '62-log' results in 62.
        String[] tokens = token.split("[^0-9]+");
        if (tokens.length == 0) {
            return 0;
        }
        try {
            return Integer.parseInt(tokens[0]);
        }
        catch (NumberFormatException e) {
            return 0;
        }
    }
}
