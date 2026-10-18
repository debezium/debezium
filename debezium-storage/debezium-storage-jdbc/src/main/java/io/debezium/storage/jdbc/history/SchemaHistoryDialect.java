/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.jdbc.history;

import java.sql.DatabaseMetaData;
import java.util.Locale;

/**
 * Builds the JDBC schema history {@code CREATE TABLE} statement for the connected database.
 * <p>
 * JDBC has no portable large-text type, and SQL Server's {@code TIMESTAMP} is {@code ROWVERSION}
 * rather than a temporal type. The statement is therefore chosen from
 * {@link DatabaseMetaData#getDatabaseProductName()}. Insert and select statements stay shared.
 */
final class SchemaHistoryDialect {

    static final String FALLBACK_HISTORY_DATA_TYPE = "VARCHAR(65000)";
    static final String TIMESTAMP_TYPE = "TIMESTAMP";

    private final String historyDataType;
    private final String timestampType;

    private SchemaHistoryDialect(String historyDataType, String timestampType) {
        this.historyDataType = historyDataType;
        this.timestampType = timestampType;
    }

    /**
     * Resolve a dialect from a database product name.
     *
     * @param databaseProductName value of {@link DatabaseMetaData#getDatabaseProductName()}; may be {@code null}
     * @return dialect for the product, never {@code null}
     */
    static SchemaHistoryDialect resolve(String databaseProductName) {
        String product = normalize(databaseProductName);
        if (product == null) {
            return general();
        }
        if (isMySqlFamily(product)) {
            return new SchemaHistoryDialect("MEDIUMTEXT", TIMESTAMP_TYPE);
        }
        if (product.contains("postgresql") || product.contains("cockroach") || product.contains("sqlite")) {
            return new SchemaHistoryDialect("TEXT", TIMESTAMP_TYPE);
        }
        if (isSqlServer(product)) {
            return new SchemaHistoryDialect("VARCHAR(MAX)", "DATETIME2");
        }
        if (isOracle(product) || isDb2(product)) {
            return new SchemaHistoryDialect("CLOB", TIMESTAMP_TYPE);
        }
        return general();
    }

    /**
     * Whether this product name is a known database.
     *
     * @param databaseProductName value of {@link DatabaseMetaData#getDatabaseProductName()}; may be {@code null}
     * @return {@code true} when a dialect mapping exists
     */
    static boolean isKnown(String databaseProductName) {
        String product = normalize(databaseProductName);
        if (product == null) {
            return false;
        }
        return isMySqlFamily(product)
                || product.contains("postgresql")
                || product.contains("cockroach")
                || product.contains("sqlite")
                || isSqlServer(product)
                || isOracle(product)
                || isDb2(product);
    }

    private static boolean isMySqlFamily(String product) {
        return product.contains("mysql") || product.contains("mariadb") || product.contains("singlestore") || product.contains("memsql");
    }

    private static boolean isSqlServer(String product) {
        return product.contains("sql server") || product.contains("azure sql");
    }

    private static boolean isOracle(String product) {
        return product.contains("oracle") && !isMySqlFamily(product);
    }

    private static String normalize(String databaseProductName) {
        return databaseProductName == null ? null : databaseProductName.toLowerCase(Locale.ROOT);
    }

    private static boolean isDb2(String product) {
        return product.equals("db2") || product.startsWith("db2/");
    }

    /**
     * Create-table statement for this database.
     *
     * @param tableName schema history table name
     * @return create-table statement, never {@code null}
     */
    String createTable(String tableName) {
        return "CREATE TABLE " + tableName +
                "(" +
                "id VARCHAR(36) NOT NULL," +
                "history_data " + historyDataType + "," +
                "history_data_seq INTEGER," +
                "record_insert_ts " + timestampType + " NOT NULL," +
                "record_insert_seq INTEGER NOT NULL," +
                "PRIMARY KEY (id, history_data_seq)" +
                ")";
    }

    private static SchemaHistoryDialect general() {
        return new SchemaHistoryDialect(FALLBACK_HISTORY_DATA_TYPE, TIMESTAMP_TYPE);
    }
}
