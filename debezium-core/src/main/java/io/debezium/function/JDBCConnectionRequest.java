package io.debezium.function;

import java.sql.SQLException;

@FunctionalInterface
public interface JDBCConnectionRequest <T> {
    T execute() throws SQLException;
}
