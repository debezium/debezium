/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.junit.jupiter;

import static org.fest.assertions.Assertions.assertThat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Objects;

import org.assertj.db.api.AbstractColumnAssert;
import org.assertj.db.api.TableAssert;
import org.assertj.db.type.ValueType;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.utility.ThrowingFunction;

/**
 * A test parameter object that represents the sink database in a JDBC end-to-end test pipeline.
 *
 * @author Chris Cranford
 */
public class Sink extends JdbcConnectionProvider {

    private final SinkType type;

    public Sink(SinkType sinkType, JdbcDatabaseContainer<?> database) {
        super(database, new SinkConnectionInitializer(sinkType));
        this.type = sinkType;
    }

    public SinkType getType() {
        return type;
    }

    public String getJdbcUrl() {
        if (SinkType.SQLSERVER == type) {
            return getContainer().getJdbcUrl() + ";databaseName=testDB";
        }
        return getContainer().getJdbcUrl();
    }

    public String formatTableName(String tableName) {
        if (type.is(SinkType.ORACLE, SinkType.DB2)) {
            return tableName.toUpperCase();
        }
        return tableName;
    }

    public String formatColumnName(String columnName) {
        if (type.is(SinkType.ORACLE, SinkType.DB2)) {
            return columnName.toUpperCase();
        }
        return columnName;
    }

    public AbstractColumnAssert assertColumnType(TableAssert table, String columnName, ValueType type, boolean lenient) {
        switch (type) {
            case BOOLEAN:
                return table.column(columnName).isBoolean(lenient);
            case TEXT:
                return table.column(columnName).isText(lenient);
            case DATE:
                return table.column(columnName).isDate(lenient);
            case TIME:
                return table.column(columnName).isTime(lenient);
            case DATE_TIME:
                return table.column(columnName).isDateTime(lenient);
            case NUMBER:
                return table.column(columnName).isNumber(lenient);
            case UUID:
                return table.column(columnName).isUUID(lenient);
            case BYTES:
                return table.column(columnName).isBytes(lenient);
            default:
                throw new RuntimeException("Unexpected value type: " + type);
        }
    }

    public void assertColumnType(TableAssert table, String columnName, ValueType type) {
        assertColumnType(table, columnName, type, false);
    }

    public void assertColumnType(TableAssert table, String columnName, ValueType type, Number... values) {
        assertColumnType(table, columnName, type, isAnyValueNull(values)).hasValues(values);
    }

    public void assertColumnType(TableAssert table, String columnName, ValueType type, String... values) {
        assertColumnType(table, columnName, type, isAnyValueNull(values)).hasValues(values);
    }

    public void assertColumnType(TableAssert table, String columnName, ValueType type, byte[]... values) {
        assertColumnType(table, columnName, type, isAnyValueNull(values)).hasValues(values);
    }

    public void assertColumn(String tableName, String columnName, String expectedType) {
        tableName = formatTableName(tableName);
        columnName = formatColumnName(columnName);
        try (ResultSet rs = getConnection().getMetaData().getColumns(null, null, tableName, columnName)) {
            if (rs.next()) {
                assertThat(rs.getString(6)).as(String.format("Column %s", columnName)).isEqualToIgnoringCase(expectedType);
                return;
            }
            throw new AssertionError(String.format("Column %s not found in table %s.", columnName, tableName));
        }
        catch (SQLException e) {
            throw new AssertionError(String.format("Failed to get column %s in table %s", columnName, tableName), e);
        }
    }

    public void assertColumn(String tableName, String columnName, String expectedType, int length) {
        tableName = formatTableName(tableName);
        columnName = formatColumnName(columnName);
        try (ResultSet rs = getConnection().getMetaData().getColumns(null, null, tableName, columnName)) {
            if (rs.next()) {
                assertThat(rs.getString(6)).isEqualToIgnoringCase(expectedType);
                assertThat(rs.getInt(7)).isEqualTo(length);
                return;
            }
            throw new AssertionError(String.format("Column %s not found in table %s.", columnName, tableName));
        }
        catch (SQLException e) {
            throw new AssertionError(String.format("Failed to get column %s in table %s", columnName, tableName), e);
        }
    }

    public void assertColumn(String tableName, String columnName, String expectedType, int precision, int scale) {
        tableName = formatTableName(tableName);
        columnName = formatColumnName(columnName);
        try (ResultSet rs = getConnection().getMetaData().getColumns(null, null, tableName, columnName)) {
            if (rs.next()) {
                assertThat(rs.getString(6)).isEqualToIgnoringCase(expectedType);
                assertThat(rs.getInt(7)).isEqualTo(precision);
                assertThat(rs.getInt(9)).isEqualTo(scale);
                return;
            }
            throw new AssertionError(String.format("Column %s not found in table %s.", columnName, tableName));
        }
        catch (SQLException e) {
            throw new AssertionError(String.format("Failed to get column %s in table %s", columnName, tableName), e);
        }
    }

    public void assertRows(String tableName, ThrowingFunction<ResultSet, Void> consumer) throws Exception {
        try (Statement st = getConnection().createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT * FROM " + tableName)) {
                assertThat(rs.next()).isTrue();
                consumer.apply(rs);
            }
        }
        catch (SQLException e) {
            throw new AssertionError("Failed to assert rows", e);
        }
    }

    @SafeVarargs
    private <T> boolean isAnyValueNull(T... values) {
        return Arrays.stream(values).anyMatch(Objects::isNull);
    }

    private static class SinkConnectionInitializer implements ConnectionInitializer {

        private final SinkType type;

        public SinkConnectionInitializer(SinkType type) {
            this.type = type;
        }

        @Override
        public void initialize(Connection connection) throws SQLException {
            if (SinkType.SQLSERVER.is(type)) {
                try (Statement statement = connection.createStatement()) {
                    statement.execute("USE testDB");
                }
            }
        }
    }

}
