/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.spi;

import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Table;

/**
 * Utility for determining (and quoting) the projected columns of snapshot SELECTs for relational connectors.
 *
 * @author Gunnar Morling
 */
public class RelationalSnapshotColumnSelector<P extends Partition> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RelationalSnapshotColumnSelector.class);

    @FunctionalInterface
    public static interface AdditionalTableFilter<P> {
        boolean test(P partition, Table table, String columnName);
    }

    private final RelationalDatabaseConnectorConfig connectorConfig;
    private final JdbcConnection jdbcConnection;
    private final AdditionalTableFilter<P> additionalFilter;

    public RelationalSnapshotColumnSelector(RelationalDatabaseConnectorConfig connectorConfig,
                                            JdbcConnection jdbcConnection) {
        this(connectorConfig, jdbcConnection, (partition, table, column) -> true);
    }

    public RelationalSnapshotColumnSelector(RelationalDatabaseConnectorConfig connectorConfig,
                                            JdbcConnection jdbcConnection, AdditionalTableFilter<P> additionalFilter) {
        this.connectorConfig = connectorConfig;
        this.jdbcConnection = jdbcConnection;
        this.additionalFilter = additionalFilter;
    }

    /**
     * Prepares a list of key columns to be used in snapshot chunk limit selects.
     *
     * @return list of snapshot select columns
     */
    public List<String> getPreparedKeyColumnNames(Table table) {
        List<Column> keyColumns = connectorConfig.getKeyMapper() != null ? connectorConfig.getKeyMapper().getKeyKolumns(table) : table.primaryKeyColumns();

        return keyColumns.stream()
                .map(column -> jdbcConnection.quotedColumnIdString(column.name()))
                .collect(Collectors.toList());
    }

    /**
     * Prepares a list of columns to be used in the snapshot select.
     * The selected columns are based on the column include/exclude filters and if all columns are excluded,
     * the list will contain all the primary key columns.
     *
     * @return list of snapshot select columns
     */
    public List<String> getPreparedColumnNames(P partition, Table table) {
        List<String> columnNames = table.retrieveColumnNames()
                .stream()
                .filter(columnName -> additionalFilter.test(partition, table, columnName))
                .filter(columnName -> connectorConfig.getColumnFilter().matches(table.id().catalog(), table.id().schema(), table.id().table(), columnName))
                .map(columnName -> jdbcConnection.quotedColumnIdString(columnName))
                .collect(Collectors.toList());

        if (columnNames.isEmpty()) {
            LOGGER.info("\t All columns in table {} were excluded due to include/exclude lists, defaulting to selecting all columns", table.id());

            columnNames = table.retrieveColumnNames()
                    .stream()
                    .map(columnName -> jdbcConnection.quotedColumnIdString(columnName))
                    .collect(Collectors.toList());
        }

        return columnNames;
    }

    public List<String> getPreparedColumnNames(Table table) {
        return getPreparedColumnNames(null, table);
    }
}
