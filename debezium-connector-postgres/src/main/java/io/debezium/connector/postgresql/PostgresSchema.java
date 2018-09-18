/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.sql.SQLException;
import java.time.ZoneOffset;
import java.util.function.Predicate;

import org.apache.kafka.connect.data.Schema;
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
 * {@link Schema} excludes any columns that have been {@link PostgresConnectorConfig#COLUMN_BLACKLIST specified} in the
 * configuration.
 *
 * @author Horia Chiorean
 */
@NotThreadSafe
public class PostgresSchema extends RelationalDatabaseSchema {

    protected final static String PUBLIC_SCHEMA_NAME = "public";
    private final static Logger LOGGER = LoggerFactory.getLogger(PostgresSchema.class);

    private final Filters filters;

    private final TypeRegistry typeRegistry;

    /**
     * Create a schema component given the supplied {@link PostgresConnectorConfig Postgres connector configuration}.
     *
     * @param config the connector configuration, which is presumed to be valid
     */
    protected PostgresSchema(PostgresConnectorConfig config, TypeRegistry typeRegistry,
            TopicSelector<TableId> topicSelector) {
        super(config, topicSelector, new Filters(config).tableFilter(),
                new Filters(config).columnFilter(), getTableSchemaBuilder(config, typeRegistry), false);

        this.filters = new Filters(config);
        this.typeRegistry = typeRegistry;
    }

    private static TableSchemaBuilder getTableSchemaBuilder(PostgresConnectorConfig config, TypeRegistry typeRegistry) {
        PostgresValueConverter valueConverter = new PostgresValueConverter(config.decimalHandlingMode(), config.temporalPrecisionMode(),
                ZoneOffset.UTC, null, config.includeUnknownDatatypes(), typeRegistry);

        return new TableSchemaBuilder(valueConverter, SchemaNameAdjuster.create(LOGGER), SourceInfo.SCHEMA);
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
        connection.readSchema(tables(), null, null, filters.tableNameFilter(), null, true);
        if (printReplicaIdentityInfo) {
            // print out all the replica identity info
            tableIds().forEach(tableId -> printReplicaIdentityInfo(connection, tableId));
        }
        // and then refresh the schemas
        refreshSchemas();
        return this;
    }

    private void printReplicaIdentityInfo(PostgresConnection connection, TableId tableId) {
        try {
            ServerInfo.ReplicaIdentity replicaIdentity = connection.readReplicaIdentityInfo(tableId);
            LOGGER.info("REPLICA IDENTITY for '{}' is '{}'; {}", tableId, replicaIdentity.toString(), replicaIdentity.description());
        } catch (SQLException e) {
            LOGGER.warn("Cannot determine REPLICA IDENTITY info for '{}'", tableId);
        }
    }

    /**
     * Refreshes this schema's content for a particular table
     *
     * @param connection a {@link JdbcConnection} instance, never {@code null}
     * @param tableId the table identifier; may not be null
     * @throws SQLException if there is a problem refreshing the schema from the database server
     */
    protected void refresh(PostgresConnection connection, TableId tableId) throws SQLException {
        Tables temp = new Tables();
        Tables.TableNameFilter tableNameFilter = Tables.filterFor(Predicate.isEqual(tableId));
        connection.readSchema(temp, null, null, tableNameFilter, null, true);

        // we expect the refreshed table to be there
        assert temp.size() == 1;
        // overwrite (add or update) or views of the tables
        tables().overwriteTable(temp.forTable(tableId));
        // and refresh the schema
        refreshSchema(tableId);
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
        return !filters.tableFilter().test(id);
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
}
