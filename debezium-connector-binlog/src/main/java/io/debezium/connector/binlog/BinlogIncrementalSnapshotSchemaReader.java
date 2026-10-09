/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import io.debezium.DebeziumException;
import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

/**
 * Reads the current definition of a table using {@code SHOW CREATE TABLE} and parses it with the
 * connector's DDL parser, so that the resulting {@link Table} model is identical to the one the
 * snapshot and streaming phases would produce.
 * <p>
 * This is used by the incremental snapshot schema fallback for tables that are missing from the
 * schema history. Reading the schema from JDBC {@code DatabaseMetaData} instead yields different
 * JDBC types for binlog connectors. For example, a {@code TIMESTAMP} column is reported as
 * {@link java.sql.Types#TIMESTAMP} with the display size (19) as its length rather than
 * {@link java.sql.Types#TIMESTAMP_WITH_TIMEZONE} with the fractional seconds precision. This
 * produces a drifting event schema ({@code io.debezium.time.NanoTimestamp} instead of
 * {@code io.debezium.time.ZonedTimestamp}), loses column defaults, and subsequently fails value
 * conversion for streamed changes (debezium/dbz#1550).
 */
final class BinlogIncrementalSnapshotSchemaReader {

    private BinlogIncrementalSnapshotSchemaReader() {
    }

    /**
     * Reads and parses the current definition of the given table.
     *
     * @param connection the JDBC connection to query the database; should not be null
     * @param schema the connector's database schema, used to parse the definition; should not be null
     * @param tableId the fully-qualified identifier of the table; should not be null
     * @return the parsed table definition; never null
     * @throws SQLException if the table definition cannot be read
     * @throws DebeziumException if the definition cannot be parsed into the requested table
     */
    static Table readSchemaViaShowCreateTable(BinlogConnectorConnection connection, BinlogDatabaseSchema<?, ?, ?, ?> schema, TableId tableId)
            throws SQLException {
        final String createTableDdl = connection.queryAndMap(
                "SHOW CREATE TABLE " + connection.quotedTableIdString(tableId),
                rs -> rs.next() ? rs.getString(2) : null);
        if (createTableDdl == null) {
            throw new DebeziumException("SHOW CREATE TABLE returned no definition for table '" + tableId + "'");
        }

        final Map<String, String> systemVariables = new HashMap<>(connection.readCharsetSystemVariables());
        systemVariables.putAll(connection.readSqlModeSystemVariable());
        final Table table = schema.parseTableDefinition(tableId, createTableDdl, systemVariables);
        if (table == null) {
            throw new DebeziumException("Failed to parse the definition of table '" + tableId + "'");
        }
        return table;
    }
}
