/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.oracle.OracleConnectorConfig.ConnectorAdapter;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.Tables.ColumnNameFilter;
import io.debezium.relational.Tables.TableFilter;

import oracle.jdbc.OracleTypes;

public class OracleConnection extends JdbcConnection {

    private final static Logger LOGGER = LoggerFactory.getLogger(OracleConnection.class);

    /**
     * Returned by column metadata in Oracle if no scale is set;
     */
    private static final int ORACLE_UNSET_SCALE = -127;

    /**
     * Pattern to identify system generated indices and column names.
     */
    private static final Pattern SYS_NC_PATTERN = Pattern.compile("^SYS_NC(?:_OID|_ROWINFO|[0-9][0-9][0-9][0-9][0-9])\\$$");

    /**
     * A field for the raw jdbc url. This field has no default value.
     */
    private static final Field URL = Field.create("url", "Raw JDBC url");

    /**
     * The database version.
     */
    private final OracleDatabaseVersion databaseVersion;

    public OracleConnection(Configuration config, Supplier<ClassLoader> classLoaderSupplier) {
        super(config, resolveConnectionFactory(config), classLoaderSupplier);

        this.databaseVersion = resolveOracleDatabaseVersion();
        LOGGER.info("Database Version: {}", databaseVersion.getBanner());
    }

    public void setSessionToPdb(String pdbName) {
        Statement statement = null;

        try {
            statement = connection().createStatement();
            statement.execute("alter session set container=" + pdbName);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        finally {
            if (statement != null) {
                try {
                    statement.close();
                }
                catch (SQLException e) {
                    LOGGER.error("Couldn't close statement", e);
                }
            }
        }
    }

    public void resetSessionToCdb() {
        Statement statement = null;

        try {
            statement = connection().createStatement();
            statement.execute("alter session set container=cdb$root");
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        finally {
            if (statement != null) {
                try {
                    statement.close();
                }
                catch (SQLException e) {
                    LOGGER.error("Couldn't close statement", e);
                }
            }
        }
    }

    public OracleDatabaseVersion getOracleVersion() {
        return databaseVersion;
    }

    private OracleDatabaseVersion resolveOracleDatabaseVersion() {
        String versionStr;
        try {
            try {
                // Oracle 18.1 introduced BANNER_FULL as the new column rather than BANNER
                // This column uses a different format than the legacy BANNER.
                versionStr = queryAndMap("SELECT BANNER_FULL FROM V$VERSION WHERE BANNER_FULL LIKE 'Oracle Database%'", (rs) -> {
                    if (rs.next()) {
                        return rs.getString(1);
                    }
                    return null;
                });
            }
            catch (SQLException e) {
                // exception ignored
                if (e.getMessage().contains("ORA-00904: \"BANNER_FULL\": invalid identifier")) {
                    LOGGER.debug("BANNER_FULL column not in V$VERSION, using BANNER column as fallback");
                    versionStr = null;
                }
                else {
                    throw e;
                }
            }

            // For databases prior to 18.1, a SQLException will be thrown due to BANNER_FULL not being a column and
            // this will cause versionStr to remain null, use fallback column BANNER for versions prior to 18.1.
            if (versionStr == null) {
                versionStr = queryAndMap("SELECT BANNER FROM V$VERSION WHERE BANNER LIKE 'Oracle Database%'", (rs) -> {
                    if (rs.next()) {
                        return rs.getString(1);
                    }
                    return null;
                });
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to resolve Oracle database version", e);
        }

        if (versionStr == null) {
            throw new RuntimeException("Failed to resolve Oracle database version");
        }

        return OracleDatabaseVersion.parse(versionStr);
    }

    @Override
    public Set<TableId> readTableNames(String databaseCatalog, String schemaNamePattern, String tableNamePattern,
                                       String[] tableTypes)
            throws SQLException {

        Set<TableId> tableIds = super.readTableNames(null, schemaNamePattern, tableNamePattern, tableTypes);

        return tableIds.stream()
                .map(t -> new TableId(databaseCatalog, t.schema(), t.table()))
                .collect(Collectors.toSet());
    }

    /**
     * Retrieves all {@code TableId} in a given database catalog, filtering certain ids that
     * should be omitted from the returned set such as special spatial tables and index-organized
     * tables.
     *
     * @param catalogName the catalog/database name
     * @return set of all table ids for existing table objects
     * @throws SQLException if a database exception occurred
     */
    protected Set<TableId> getAllTableIds(String catalogName) throws SQLException {
        final String query = "select owner, table_name from all_tables " +
        // filter special spatial tables
                "where table_name NOT LIKE 'MDRT_%' " +
                "and table_name NOT LIKE 'MDRS_%' " +
                "and table_name NOT LIKE 'MDXT_%' " +
                // filter index-organized-tables
                "and (table_name NOT LIKE 'SYS_IOT_OVER_%' and IOT_NAME IS NULL) ";

        Set<TableId> tableIds = new HashSet<>();
        query(query, (rs) -> {
            while (rs.next()) {
                tableIds.add(new TableId(catalogName, rs.getString(1), rs.getString(2)));
            }
            LOGGER.trace("TableIds are: {}", tableIds);
        });

        return tableIds;
    }

    // todo replace metadata with something like this
    private ResultSet getTableColumnsInfo(String schemaNamePattern, String tableName) throws SQLException {
        String columnQuery = "select column_name, data_type, data_length, data_precision, data_scale, default_length, density, char_length from " +
                "all_tab_columns where owner like '" + schemaNamePattern + "' and table_name='" + tableName + "'";

        PreparedStatement statement = connection().prepareStatement(columnQuery);
        return statement.executeQuery();
    }

    // this is much faster, we will use it until full replacement of the metadata usage TODO
    public void readSchemaForCapturedTables(Tables tables, String databaseCatalog, String schemaNamePattern,
                                            ColumnNameFilter columnFilter, boolean removeTablesNotFoundInJdbc, Set<TableId> capturedTables)
            throws SQLException {

        Set<TableId> tableIdsBefore = new HashSet<>(tables.tableIds());

        DatabaseMetaData metadata = connection().getMetaData();
        Map<TableId, List<Column>> columnsByTable = new HashMap<>();

        for (TableId tableId : capturedTables) {
            try (ResultSet columnMetadata = metadata.getColumns(databaseCatalog, schemaNamePattern, tableId.table(), null)) {
                while (columnMetadata.next()) {
                    // add all whitelisted columns
                    readTableColumn(columnMetadata, tableId, columnFilter).ifPresent(column -> {
                        columnsByTable.computeIfAbsent(tableId, t -> new ArrayList<>())
                                .add(column.create());
                    });
                }
            }
        }

        // Read the metadata for the primary keys ...
        for (Map.Entry<TableId, List<Column>> tableEntry : columnsByTable.entrySet()) {
            // First get the primary key information, which must be done for *each* table ...
            List<String> pkColumnNames = readPrimaryKeyNames(metadata, tableEntry.getKey());

            // Then define the table ...
            List<Column> columns = tableEntry.getValue();
            Collections.sort(columns);
            tables.overwriteTable(tableEntry.getKey(), columns, pkColumnNames, null);
        }

        if (removeTablesNotFoundInJdbc) {
            // Remove any definitions for tables that were not found in the database metadata ...
            tableIdsBefore.removeAll(columnsByTable.keySet());
            tableIdsBefore.forEach(tables::removeTable);
        }

        for (TableId tableId : capturedTables) {
            overrideOracleSpecificColumnTypes(tables, tableId, tableId);
        }

    }

    @Override
    public void readSchema(Tables tables, String databaseCatalog, String schemaNamePattern, TableFilter tableFilter,
                           ColumnNameFilter columnFilter, boolean removeTablesNotFoundInJdbc)
            throws SQLException {

        super.readSchema(tables, null, schemaNamePattern, tableFilter, columnFilter, removeTablesNotFoundInJdbc);

        Set<TableId> tableIds = tables.tableIds().stream().filter(x -> schemaNamePattern.equals(x.schema())).collect(Collectors.toSet());

        for (TableId tableId : tableIds) {
            // super.readSchema() populates ids without the catalog; hence we apply the filtering only
            // here and if a table is included, overwrite it with a new id including the catalog
            TableId tableIdWithCatalog = new TableId(databaseCatalog, tableId.schema(), tableId.table());

            if (tableFilter.isIncluded(tableIdWithCatalog)) {
                overrideOracleSpecificColumnTypes(tables, tableId, tableIdWithCatalog);
            }

            tables.removeTable(tableId);
        }
    }

    @Override
    public List<String> readTableUniqueIndices(DatabaseMetaData metadata, TableId id) throws SQLException {
        return super.readTableUniqueIndices(metadata, id.toDoubleQuoted());
    }

    @Override
    protected boolean isTableUniqueIndexIncluded(String indexName, String columnName) {
        if (columnName != null) {
            return !SYS_NC_PATTERN.matcher(columnName).matches();
        }
        return false;
    }

    private void overrideOracleSpecificColumnTypes(Tables tables, TableId tableId, TableId tableIdWithCatalog) {
        TableEditor editor = tables.editTable(tableId);
        editor.tableId(tableIdWithCatalog);

        List<String> columnNames = new ArrayList<>(editor.columnNames());
        for (String columnName : columnNames) {
            Column column = editor.columnWithName(columnName);
            if (column.jdbcType() == Types.TIMESTAMP) {
                editor.addColumn(
                        column.edit()
                                .length(column.scale().orElse(Column.UNSET_INT_VALUE))
                                .scale(null)
                                .create());
            }
            // NUMBER columns without scale value have it set to -127 instead of null;
            // let's rectify that
            else if (column.jdbcType() == OracleTypes.NUMBER) {
                column.scale()
                        .filter(s -> s == ORACLE_UNSET_SCALE)
                        .ifPresent(s -> {
                            editor.addColumn(
                                    column.edit()
                                            .scale(null)
                                            .create());
                        });
            }
        }
        tables.overwriteTable(editor.create());
    }

    /**
     * Resolve the table name case insensitivity used by the schema based on the following rules:
     *
     * <ul>
     *     <li>If option is provided in connector configuration, it will be used.</li>
     *     <li>Otherwise resolved by database version where Oracle 11 will be {@code true}; otherwise {@code false}.</li>
     * </ul>
     *
     * @param connectorConfig the connector configuration
     * @return whether table name case insensitivity is used
     */
    public boolean getTablenameCaseInsensitivity(OracleConnectorConfig connectorConfig) {
        Optional<Boolean> configValue = connectorConfig.getTablenameCaseInsensitive();
        return configValue.orElse(getOracleVersion().getMajor() == 11);
    }

    public OracleConnection executeLegacy(String... sqlStatements) throws SQLException {
        return executeLegacy(statement -> {
            for (String sqlStatement : sqlStatements) {
                if (sqlStatement != null) {
                    statement.execute(sqlStatement);
                }
            }
        });
    }

    public OracleConnection executeLegacy(Operations operations) throws SQLException {
        Connection conn = connection();
        try (Statement statement = conn.createStatement()) {
            operations.apply(statement);
            commit();
        }
        return this;
    }

    public static String connectionString(Configuration config) {
        return config.getString(URL) != null ? config.getString(URL)
                : ConnectorAdapter.parse(config.getString("connection.adapter")).getConnectionUrl();
    }

    private static ConnectionFactory resolveConnectionFactory(Configuration config) {
        return JdbcConnection.patternBasedFactory(connectionString(config));
    }
}
