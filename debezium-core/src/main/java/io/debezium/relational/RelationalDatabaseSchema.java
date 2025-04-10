/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.relational.Key.KeyMapper;
import io.debezium.relational.Tables.ColumnNameFilter;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.mapping.ColumnMappers;
import io.debezium.schema.DatabaseSchema;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * A {@link DatabaseSchema} of a relational database such as Postgres. Provides information about the physical structure
 * of the database (the "database schema") as well as the structure of corresponding CDC messages (the "event schema").
 *
 * @author Gunnar Morling
 */
public abstract class RelationalDatabaseSchema implements DatabaseSchema<TableId> {
    private final static Logger LOG = LoggerFactory.getLogger(RelationalDatabaseSchema.class);

    private final TopicNamingStrategy<TableId> topicNamingStrategy;
    private final TableSchemaBuilder schemaBuilder;
    private final TableFilter tableFilter;
    private final ColumnNameFilter columnFilter;
    private final ColumnMappers columnMappers;
    private final KeyMapper customKeysMapper;

    private final SchemasByTableId schemasByTableId;
    private final Tables tables;

    protected RelationalDatabaseSchema(RelationalDatabaseConnectorConfig config, TopicNamingStrategy<TableId> topicNamingStrategy,
                                       TableFilter tableFilter, ColumnNameFilter columnFilter, TableSchemaBuilder schemaBuilder,
                                       boolean tableIdCaseInsensitive, KeyMapper customKeysMapper) {

        this.topicNamingStrategy = topicNamingStrategy;
        this.schemaBuilder = schemaBuilder;
        this.tableFilter = tableFilter;
        this.columnFilter = columnFilter;
        this.columnMappers = ColumnMappers.create(config);
        this.customKeysMapper = customKeysMapper;

        this.schemasByTableId = new SchemasByTableId(tableIdCaseInsensitive);
        this.tables = new Tables(tableIdCaseInsensitive);
    }

    @Override
    public void close() {
    }

    /**
     * Returns the set of table ids included in the current filter configuration.
     */
    public Set<TableId> tableIds() {
        // TODO that filtering should really be done once upon insertion
        return tables.subset(tableFilter).tableIds();
    }

    @Override
    public void assureNonEmptySchema(boolean failOnNoTables) {
        if (tableIds().isEmpty()) {
            LOG.warn(NO_CAPTURED_DATA_COLLECTIONS_WARNING);
            if (failOnNoTables) {
                throw new ConnectException(NO_CAPTURED_DATA_COLLECTIONS_WARNING);
            }
        }
    }

    /**
     * Get the {@link TableSchema Schema information} for the table with the given identifier, if that table exists and
     * is included by the filter configuration.
     * <p>
     * Note that the {@link Schema} will not contain any columns that have been filtered out.
     *
     * @param id
     *            the table identifier; may be null
     * @return the schema information, or null if there is no table with the given identifier, if the identifier is
     *         null, or if the table has been excluded by the filters
     */
    @Override
    public TableSchema schemaFor(TableId id) {
        return schemasByTableId.get(id);
    }

    /**
     * Get the {@link Table} meta-data for the table with the given identifier, if that table exists and is
     * included by the filter configuration
     *
     * @param id the table identifier; may be null
     * @return the current table definition, or null if there is no table with the given identifier, if the identifier is null,
     *         or if the table has been excluded by the filters
     */
    public Table tableFor(TableId id) {
        return tableFilter.isIncluded(id) ? tables.forTable(id) : null;
    }

    @Override
    public boolean isHistorized() {
        return false;
    }

    protected Tables tables() {
        return tables;
    }

    protected void clearSchemas() {
        schemasByTableId.clear();
    }

    /**
     * Builds up the CDC event schema for the given table and stores it in this schema.
     */
    protected void buildAndRegisterSchema(Table table) {
        if (tableFilter.isIncluded(table.id())) {
            TableSchema schema = schemaBuilder.create(topicNamingStrategy, table, columnFilter, columnMappers, customKeysMapper);
            schemasByTableId.put(table.id(), schema);
        }
    }

    protected void removeSchema(TableId id) {
        schemasByTableId.remove(id);
    }

    /**
     * A map of schemas by table id. Table names are stored lower-case if required as per the config.
     */
    private static class SchemasByTableId {

        private final boolean tableIdCaseInsensitive;
        private final ConcurrentMap<TableId, TableSchema> values;

        SchemasByTableId(boolean tableIdCaseInsensitive) {
            this.tableIdCaseInsensitive = tableIdCaseInsensitive;
            this.values = new ConcurrentHashMap<>();
        }

        public void clear() {
            values.clear();
        }

        public TableSchema remove(TableId tableId) {
            return values.remove(toLowerCaseIfNeeded(tableId));
        }

        public TableSchema get(TableId tableId) {
            return values.get(toLowerCaseIfNeeded(tableId));
        }

        public TableSchema put(TableId tableId, TableSchema updated) {
            return values.put(toLowerCaseIfNeeded(tableId), updated);
        }

        private TableId toLowerCaseIfNeeded(TableId tableId) {
            return tableIdCaseInsensitive ? tableId.toLowercase() : tableId;
        }
    }

    protected TableFilter getTableFilter() {
        return tableFilter;
    }

    @Override
    public boolean tableInformationComplete() {
        return false;
    }

    /**
     * Refreshes the schema content with a table constructed externally
     *
     * @param table constructed externally - typically from decoder metadata or an external signal
     */
    public void refresh(Table table) {
        // overwrite (add or update) or views of the tables
        tables().overwriteTable(table);
        // and refresh the schema
        refreshSchema(table.id());
    }

    protected void refreshSchema(TableId id) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("refreshing DB schema for table '{}'", id);
        }
        Table table = tableFor(id);

        buildAndRegisterSchema(table);
    }
}
