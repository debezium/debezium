/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.junit;

import static org.assertj.db.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.sql.DataSource;

import org.assertj.db.api.TableAssert;
import org.assertj.db.type.Source;
import org.assertj.db.type.Table;

/**
 * @author Chris Cranford
 */
public class TestHelper {

    private TestHelper() {
    }

    /**
     * Controls whether the source and sink databases should be queried to output their table state
     *
     * @return true to query the database state, false otherwise (the default).
     */
    public static boolean shouldQueryDatabaseState() {
        return Boolean.parseBoolean(System.getProperty("query.database.state", "false"));
    }

    /**
     * Allows controlling test behavior on whether data originates from snapshot phase or streaming
     * phase of the end to end integration tests.
     *
     * @return true to use snapshot, false otherwise (the default).
     */
    public static boolean isSourceSnapshot() {
        return Boolean.parseBoolean(System.getProperty("source.snapshot", "false"));
    }

    /**
     * Allows controlling test behavior on whether default values should be included on the source
     * definition side of integration end to end tests.
     *
     * @return true to use default values, false otherwise (the default).
     */
    public static boolean isDefaultValuesEnabled() {
        return Boolean.parseBoolean(System.getProperty("source.use.default.values", "false"));
    }

    /**
     * Allows controlling test behavior on whether to manipulate connection timezone details for
     * databases such as MySQL where the use of {@code connectionTimeZone} has implications on
     * how the value is returned via JDBC.
     *
     * @return true if the connection time zone should be applied, otherwise use default driver behavior.
     */
    public static boolean isConnectionTimeZoneUsed() {
        return Boolean.parseBoolean(System.getProperty("connection.time_zone", "true"));
    }

    public static String getSourceTimeZone() {
        return System.getProperty("source.time_zone", "UTC");
    }

    public static String getSinkTimeZone() {
        return System.getProperty("sink.time_zone", "UTC");
    }

    public static TableAssert assertTable(DataSource dataSource, String tableName) {
        return assertThat(new Table(dataSource, tableName));
    }

    public static TableAssert assertTable(Source source, String tableName) {
        return assertThat(new Table(source, tableName));
    }

    /**
     * Get the nested root-cause of the exception.
     *
     * @param t the exception, should not be {@code null}
     * @return the nested root-cause exception
     */
    public static Throwable getRootCause(Throwable t) {
        Throwable result = t;
        while (result.getCause() != null) {
            result = t.getCause();
        }
        return result;
    }

    public static List<String> getPrimaryKeyColumnNames(DataSource dataSource, String tableName) {
        try (Connection connection = dataSource.getConnection()) {
            final Map<Integer, String> primaryKeyColumnNames = new TreeMap<>();
            try (ResultSet rs = connection.getMetaData().getPrimaryKeys(null, null, tableName)) {
                while (rs.next()) {
                    primaryKeyColumnNames.put(rs.getInt(5), rs.getString(4));
                }
            }
            return new ArrayList<>(primaryKeyColumnNames.values());
        }
        catch (SQLException e) {
            throw new IllegalStateException("Failed to read table '" + tableName + "' primary key columns", e);
        }
    }

}
