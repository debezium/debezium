/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ServerInfo;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.Tables;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;

/**
 * Component that records the schema information for the {@link PostgresConnector}. The schema information contains
 * the {@link Tables table definitions} and the Kafka Connect {@link #schemaFor(TableId) Schema}s for each table, where the
 * {@link Schema} excludes any columns that have been {@link PostgresConnectorConfig#COLUMN_EXCLUDE_LIST specified} in the
 * configuration.
 *
 * @author Horia Chiorean
 */
@NotThreadSafe
public class PostgresSchema extends RelationalDatabaseSchema {

    protected final static String PUBLIC_SCHEMA_NAME = "public";
    private final static Logger LOGGER = LoggerFactory.getLogger(PostgresSchema.class);

    private final TypeRegistry typeRegistry;

    private final Map<TableId, List<String>> tableIdToToastableColumns;
    private final Map<Integer, TableId> relationIdToTableId;
    private final boolean readToastableColumns;

    /**
     * Create a schema component given the supplied {@link PostgresConnectorConfig Postgres connector configuration}.
     *
     * @param config the connector configuration, which is presumed to be valid
     */
    protected PostgresSchema(PostgresConnectorConfig config, TypeRegistry typeRegistry, Charset databaseCharset,
                             TopicSelector<TableId> topicSelector) {
        super(config, topicSelector, new Filters(config).tableFilter(),
                new Filters(config).columnFilter(), getTableSchemaBuilder(config, typeRegistry, databaseCharset), false,
                config.getKeyMapper());

        this.typeRegistry = typeRegistry;
        this.tableIdToToastableColumns = new HashMap<>();
        this.relationIdToTableId = new HashMap<>();
        this.readToastableColumns = config.skipRefreshSchemaOnMissingToastableData();
    }

    private static TableSchemaBuilder getTableSchemaBuilder(PostgresConnectorConfig config, TypeRegistry typeRegistry, Charset databaseCharset) {
        PostgresValueConverter valueConverter = new PostgresValueConverter(
                databaseCharset,
                config.getDecimalMode(),
                config.getTemporalPrecisionMode(),
                ZoneOffset.UTC,
                null,
                config.includeUnknownDatatypes(),
                typeRegistry,
                config.hStoreHandlingMode(),
                config.binaryHandlingMode(),
                config.intervalHandlingMode(),
                config.toastedValuePlaceholder());

        return new TableSchemaBuilder(valueConverter, SchemaNameAdjuster.create(LOGGER), config.customConverterRegistry(), config.getSourceInfoStructMaker().schema(),
                config.getSanitizeFieldNames());
    }

    /**
     * Initializes the content for this schema by reading all the database information from the supplied connection.
     *
     * @param connection a {@link JdbcConnection} instance, never {@code null}
     * @param printReplicaIdentityInfo whether or not to look and print out replica identity information about the tables
     * @return this object so methods can be chained together; never null
     * @throws SQLException if there is a problem obtaining the schema from the database server
     */
    protected PostgresSchema refresh(PostgresConnection connection, boolean printReplicaIdentityInfo) throws SQLException {
        // read all the information from the DB
        connection.readSchema(tables(), null, null, getTableFilter(), null, true);
        if (printReplicaIdentityInfo) {
            // print out all the replica identity info
            tableIds().forEach(tableId -> printReplicaIdentityInfo(connection, tableId));
        }
        // and then refresh the schemas
        refreshSchemas();
        if (readToastableColumns) {
            tableIds().forEach(tableId -> refreshToastableColumnsMap(connection, tableId));
        }
        return this;
    }

    private void printReplicaIdentityInfo(PostgresConnection connection, TableId tableId) {
        try {
            ServerInfo.ReplicaIdentity replicaIdentity = connection.readReplicaIdentityInfo(tableId);
            LOGGER.info("REPLICA IDENTITY for '{}' is '{}'; {}", tableId, replicaIdentity, replicaIdentity.description());
        }
        catch (SQLException e) {
            LOGGER.warn("Cannot determine REPLICA IDENTITY info for '{}'", tableId);
        }
    }

    /**
     * Refreshes this schema's content for a particular table
     *
     * @param connection a {@link JdbcConnection} instance, never {@code null}
     * @param tableId the table identifier; may not be null
     * @param refreshToastableColumns refreshes the cache of toastable columns for `tableId`, if {@code true}
     * @throws SQLException if there is a problem refreshing the schema from the database server
     */
    protected void refresh(PostgresConnection connection, TableId tableId, boolean refreshToastableColumns) throws SQLException {
        Tables temp = new Tables();
        connection.readSchema(temp, null, null, tableId::equals, null, true);

        // the table could be deleted before the event was processed
        if (temp.size() == 0) {
            LOGGER.warn("Refresh of {} was requested but the table no longer exists", tableId);
            return;
        }
        // overwrite (add or update) or views of the tables
        tables().overwriteTable(temp.forTable(tableId));
        // refresh the schema
        refreshSchema(tableId);

        if (refreshToastableColumns) {
            // and refresh toastable columns info
            refreshToastableColumnsMap(connection, tableId);
        }
    }

    /**
     * Refreshes the schema content with a table constructed externally
     *
     * @param table constructed externally - typically from decoder metadata
     */
    protected void refresh(Table table) {
        // overwrite (add or update) or views of the tables
        tables().overwriteTable(table);
        // and refresh the schema
        refreshSchema(table.id());
    }

    protected boolean isFilteredOut(TableId id) {
        return !getTableFilter().isIncluded(id);
    }

    /**
     * Discard any currently-cached schemas and rebuild them using the filters.
     */
    protected void refreshSchemas() {
        clearSchemas();

        // Create TableSchema instances for any existing table ...
        tableIds().forEach(this::refreshSchema);
    }

    private void refreshSchema(TableId id) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("refreshing DB schema for table '{}'", id);
        }
        Table table = tableFor(id);

        buildAndRegisterSchema(table);
    }

    private void refreshToastableColumnsMap(PostgresConnection connection, TableId tableId) {
        // This method populates the list of 'toastable' columns for `tableId`.
        // A toastable column is one that has storage strategy 'x' (inline-compressible + secondary storage enabled),
        // 'e' (secondary storage enabled), or 'm' (inline-compressible).
        //
        // Note that, rather confusingly, the 'm' storage strategy does in fact permit secondary storage, but only as a
        // last resort.
        //
        // Also, we cannot account for the possibility that future versions of PostgreSQL introduce new storage strategies
        // that include secondary storage. We should move to native decoding in PG 10 and get rid of this hacky code
        // before that possibility is realized.

        // Collect the non-system (attnum > 0), present (not attisdropped) column names that are toastable.
        //
        // NOTE (Ian Axelrod):
        // I Would prefer to use data provided by PgDatabaseMetaData, but the PG JDBC driver does not expose storage type
        // information. Thus, we need to make a separate query. If we are refreshing schemas rarely, this is not a big
        // deal.
        List<String> toastableColumns = new ArrayList<>();
        String relName = tableId.table();
        String schema = tableId.schema() != null && tableId.schema().length() > 0 ? tableId.schema() : "public";
        String statement = "select att.attname" +
                " from pg_attribute att " +
                " join pg_class tbl on tbl.oid = att.attrelid" +
                " join pg_namespace ns on tbl.relnamespace = ns.oid" +
                " where tbl.relname = ?" +
                " and ns.nspname = ?" +
                " and att.attnum > 0" +
                " and att.attstorage in ('x', 'e', 'm')" +
                " and not att.attisdropped;";

        try {
            connection.prepareQuery(statement, stmt -> {
                stmt.setString(1, relName);
                stmt.setString(2, schema);
            }, rs -> {
                while (rs.next()) {
                    toastableColumns.add(rs.getString(1));
                }
            });
            if (!connection.connection().getAutoCommit()) {
                connection.connection().commit();
            }
        }
        catch (SQLException e) {
            throw new ConnectException("Unable to refresh toastable columns mapping", e);
        }

        tableIdToToastableColumns.put(tableId, Collections.unmodifiableList(toastableColumns));
    }

    protected static TableId parse(String table) {
        TableId tableId = TableId.parse(table, false);
        if (tableId == null) {
            return null;
        }
        return tableId.schema() == null ? new TableId(tableId.catalog(), PUBLIC_SCHEMA_NAME, tableId.table()) : tableId;
    }

    public TypeRegistry getTypeRegistry() {
        return typeRegistry;
    }

    public List<String> getToastableColumnsForTableId(TableId tableId) {
        return tableIdToToastableColumns.getOrDefault(tableId, Collections.emptyList());
    }

    /**
     * Applies schema changes for the specified table.
     *
     * @param relationId the postgres relation unique identifier for the table
     * @param table externally constructed table, typically from the decoder; must not be null
     */
    public void applySchemaChangesForTable(int relationId, Table table) {
        assert table != null;

        if (isFilteredOut(table.id())) {
            LOGGER.trace("Skipping schema refresh for table '{}' with relation '{}' as table is filtered", table.id(), relationId);
            return;
        }

        relationIdToTableId.put(relationId, table.id());
        refresh(table);
    }

    /**
     * Resolve a {@link Table} based on a supplied table relation unique identifier.
     * <p>
     * This implementation relies on a prior call to {@link #applySchemaChangesForTable(int, Table)} to have
     * applied schema changes from a replication stream with the {@code relationId} for the relationship to exist
     * and be capable of lookup.
     *
     * @param relationId the unique table relation identifier
     * @return the resolved table or null
     */
    public Table tableFor(int relationId) {
        TableId tableId = relationIdToTableId.get(relationId);
        if (tableId == null) {
            LOGGER.debug("Relation '{}' is unknown, cannot resolve to table", relationId);
            return null;
        }
        LOGGER.debug("Relation '{}' resolved to table '{}'", relationId, tableId);
        return tableFor(tableId);
    }

    @Override
    public boolean tableInformationComplete() {
        // PostgreSQL does not support HistorizedDatabaseSchema - so no tables are recovered
        return false;
    }
}
