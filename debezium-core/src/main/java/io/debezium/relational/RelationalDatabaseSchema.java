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

import io.debezium.config.CommonConnectorConfig;
import io.debezium.relational.Tables.ColumnNameFilter;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.mapping.ColumnMappers;
import io.debezium.schema.DatabaseSchema;
import io.debezium.schema.TopicSelector;

/**
 * A {@link DatabaseSchema} of a relational database such as Postgres. Provides information about the physical structure
 * of the database (the "database schema") as well as the structure of corresponding CDC messages (the "event schema").
 *
 * @author Gunnar Morling
 */
public abstract class RelationalDatabaseSchema implements DatabaseSchema<TableId> {

    private final TopicSelector<TableId> topicSelector;
    private final TableSchemaBuilder schemaBuilder;
    private final TableFilter tableFilter;
    private final ColumnNameFilter columnFilter;
    private final ColumnMappers columnMappers;

    private final String schemaPrefix;
    private final SchemasByTableId schemasByTableId;
    private final Tables tables;

    protected RelationalDatabaseSchema(CommonConnectorConfig config, TopicSelector<TableId> topicSelector,
            TableFilter tableFilter, ColumnNameFilter columnFilter, TableSchemaBuilder schemaBuilder,
            boolean tableIdCaseInsensitive) {

        this.topicSelector = topicSelector;
        this.schemaBuilder = schemaBuilder;
        this.tableFilter = tableFilter;
        this.columnFilter = columnFilter;
        this.columnMappers = ColumnMappers.create(config.getConfig());

        this.schemaPrefix = getSchemaPrefix(config.getLogicalName());
        this.schemasByTableId = new SchemasByTableId(tableIdCaseInsensitive);
        this.tables = new Tables(tableIdCaseInsensitive);
    }

    private static String getSchemaPrefix(String serverName) {
        if (serverName == null) {
            return "";
        }
        else {
            serverName = serverName.trim();
            return serverName.endsWith(".") || serverName.isEmpty() ? serverName : serverName + ".";
        }
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
            TableSchema schema = schemaBuilder.create(schemaPrefix, getEnvelopeSchemaName(table), table, columnFilter, columnMappers);
            schemasByTableId.put(table.id(), schema);
        }
    }

    protected void removeSchema(TableId id) {
        schemasByTableId.remove(id);
    }

    private String getEnvelopeSchemaName(Table table) {
        return topicSelector.topicNameFor(table.id()) + ".Envelope";
    }


    /**
     * A map of schemas by table id. Table names are stored lower-case if required as per the config.
     */
    private static class SchemasByTableId {

        private final boolean tableIdCaseInsensitive;
        private final ConcurrentMap<TableId, TableSchema> values;

        public SchemasByTableId(boolean tableIdCaseInsensitive) {
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
}
