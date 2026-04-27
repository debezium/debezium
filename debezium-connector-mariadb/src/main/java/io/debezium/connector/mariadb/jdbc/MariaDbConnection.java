/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.jdbc;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.binlog.gtid.GtidSet;
import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.connector.binlog.jdbc.BinlogFieldReader;
import io.debezium.connector.binlog.jdbc.ConnectionConfiguration;
import io.debezium.connector.mariadb.gtid.MariaDbGtidSet;
import io.debezium.connector.mariadb.gtid.MariaDbGtidSet.MariaDbGtid;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.ColumnNameFilter;
import io.debezium.relational.Tables.TableFilter;

/**
 * A concrete implementation of {@link BinlogConnectorConnection} for MariaDB.
 * @author Chris Cranford
 */
public class MariaDbConnection extends BinlogConnectorConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(MariaDbConnection.class);

    public MariaDbConnection(ConnectionConfiguration configuration, BinlogFieldReader fieldReader) {
        super(configuration, fieldReader);
    }

    @Override
    public boolean isGtidModeEnabled() {
        // Always has GTID enabled.
        // GTID_STRICT_MODE can be enabled or disabled.
        return true;
    }

    @Override
    public GtidSet knownGtidSet() {
        // MariaDB does not store the executed GTID details in the SHOW MASTER STATUS output like MySQL;
        // however, instead makes this information available as a variable. The GTID_BINLOG_POS gives
        // the current GTID position of the binary log and can therefore be considered the equivalent to
        // MySQL's executed GTID set.
        try {
            return queryAndMap("SHOW GLOBAL VARIABLES LIKE 'GTID_BINLOG_POS'", rs -> {
                if (rs.next()) {
                    return new MariaDbGtidSet(rs.getString(2));
                }
                return new MariaDbGtidSet("");
            });
        }
        catch (SQLException e) {
            throw new DebeziumException("Unexpected error while looking at GTID_BINLOG_POS: ", e);
        }
    }

    @Override
    public GtidSet subtractGtidSet(GtidSet set1, GtidSet set2) {
        return set1.subtract(set2);
    }

    @Override
    public GtidSet purgedGtidSet() {
        // The MariaDB community mentioned we could get the purged GTID values from the GTID_LIST_EVENT; however,
        // this value is only available after we connect and would require a temporary binlog connection to get
        // the data, so for now simply returning an empty set until we split the code base.
        return new MariaDbGtidSet("");
    }

    @Override
    public GtidSet filterGtidSet(Predicate<String> gtidSourceFilter, String offsetGtids, GtidSet availableServerGtidSet, GtidSet purgedServerGtidSet) {
        String gtidStr = offsetGtids;
        if (gtidStr == null) {
            return null;
        }

        LOGGER.info("Attempting to generate a filtered GTID set");
        LOGGER.info("GTID set from previous recorded offset: {}", gtidStr);

        MariaDbGtidSet filteredGtidSet = new MariaDbGtidSet(gtidStr);
        if (gtidSourceFilter != null) {
            filteredGtidSet = (MariaDbGtidSet) filteredGtidSet.retainAll(gtidSourceFilter);
            LOGGER.info("GTID set after applying GTID source includes/excludes to previous recorded offset: {}", filteredGtidSet);
        }

        LOGGER.info("GTID set available on server: {}", availableServerGtidSet);

        final MariaDbGtidSet knownGtidSet = filteredGtidSet;
        LOGGER.info("Using first available positions for new GTID channels");

        final GtidSet relevantAvailableServerGtidSet = (gtidSourceFilter != null) ? availableServerGtidSet.retainAll(gtidSourceFilter) : availableServerGtidSet;
        LOGGER.info("Relevant GTID set available on server: {}", relevantAvailableServerGtidSet);

        GtidSet mergedGtidSet = relevantAvailableServerGtidSet
                .retainAll(serverId -> {
                    // ServerId in this context is "<domain-id>-<server-id>"
                    final MariaDbGtid compliantGtid = MariaDbGtid.parse(serverId + "-0");
                    return knownGtidSet.forGtidStream(compliantGtid) != null;
                })
                .with(purgedServerGtidSet)
                .with(filteredGtidSet);

        LOGGER.info("Final merged GTID set to use when connecting to MariaDB: {}", mergedGtidSet);
        return mergedGtidSet;
    }

    @Override
    protected Map<TableId, List<Column>> getColumnsDetails(String catalogName, String schemaName,
                                                           String tableName, TableFilter tableFilter, ColumnNameFilter columnFilter, DatabaseMetaData metadata,
                                                           final Set<TableId> viewIds, boolean throwOnCaseInsensitiveColumnMismatch)
            throws SQLException {
        Map<TableId, List<Column>> columnsByTable = super.getColumnsDetails(catalogName, schemaName, tableName,
                tableFilter, columnFilter, metadata, viewIds, throwOnCaseInsensitiveColumnMismatch);

        // Fetch explicitly synthetic columns that the JDBC driver omitted (e.g., MariaDB DB_ROW_HASH for unique generated columns)
        for (Map.Entry<TableId, List<Column>> entry : columnsByTable.entrySet()) {
            TableId tableId = entry.getKey();
            List<Column> columns = entry.getValue();

            try {
                String sql = "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, "
                        + "NUMERIC_SCALE, IS_NULLABLE, ORDINAL_POSITION "
                        + "FROM INFORMATION_SCHEMA.COLUMNS "
                        + "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "
                        + "AND (EXTRA LIKE '%GENERATED%' OR EXTRA LIKE '%INVISIBLE%')";

                prepareQueryAndMap(sql, ps -> {
                    ps.setString(1, tableId.catalog());
                    ps.setString(2, tableId.table());
                }, rs -> {
                    while (rs.next()) {
                        String columnName = rs.getString(1);
                        // Only process it if the JDBC driver missed it completely
                        boolean alreadyGenerated = columns.stream().anyMatch(c -> c.name().equals(columnName));
                        if (!alreadyGenerated) {
                            boolean isSynthetic = false;
                            if (columnName.startsWith("DB_ROW_HASH")) {
                                LOGGER.debug("Including DB_ROW_HASH as synthetic column for {}: {}", tableId, columnName);
                                isSynthetic = true;
                            }

                            LOGGER.debug("Injecting synthetic/generated column intelligently for {}: {}", tableId, columnName);

                            // Check explicit exclusions to respect explicitly excluded flags Native to Debezium
                            if (columnFilter != null && !columnFilter.matches(tableId.catalog(), tableId.schema(), tableId.table(), columnName)) {
                                LOGGER.debug("Explicitly excluded synthetic column for {}: {}", tableId, columnName);
                                continue;
                            }

                            ColumnEditor editor = Column.editor()
                                    .name(columnName)
                                    .type(rs.getString(2))
                                    .length(rs.getInt(3) == 0 ? -1 : rs.getInt(3))
                                    .scale(rs.getInt(5) == 0 ? null : rs.getInt(5))
                                    .optional("YES".equalsIgnoreCase(rs.getString(6)))
                                    .position(rs.getInt(7))
                                    .synthetic(isSynthetic)
                                    .generated(!isSynthetic);

                            columns.add(editor.create());
                        }
                    }
                    return null;
                });

                // Ensure columns are sorted by physical ORDINAL_POSITION naturally
                Collections.sort(columns);
                for (int i = 0; i < columns.size(); i++) {
                    Column col = columns.get(i);
                    columns.set(i, col.edit().position(i + 1).create());
                }

            }
            catch (SQLException e) {
                LOGGER.warn("Failed to natively augment synthetic columns metadata for {}: {}", tableId, e.getMessage());
            }
        }
        return columnsByTable;
    }

}
