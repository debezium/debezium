/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.starrocks;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

/**
 * Test utilities for StarRocks.
 */
final class StarRocksTestUtils {

    private StarRocksTestUtils() {
    }

    /**
     * The StarRocks JDBC driver does not expose primary keys through the JDBC metadata API,
     * as {@code SHOW KEYS} returns an empty result for StarRocks PRIMARY KEY tables; the key
     * columns are read from {@code information_schema} instead.
     */
    static List<String> getPrimaryKeyColumnNames(DataSource dataSource, String tableName) {
        final List<String> primaryKeyColumnNames = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT column_name FROM information_schema.columns " +
                                "WHERE table_schema = DATABASE() AND table_name = ? AND column_key = 'PRI' " +
                                "ORDER BY ordinal_position")) {
            statement.setString(1, tableName);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    primaryKeyColumnNames.add(rs.getString(1));
                }
            }
            return primaryKeyColumnNames;
        }
        catch (SQLException e) {
            throw new IllegalStateException("Failed to read table '" + tableName + "' primary key columns", e);
        }
    }
}
