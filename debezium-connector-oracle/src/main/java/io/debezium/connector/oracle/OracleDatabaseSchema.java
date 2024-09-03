/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.StreamingAdapter.TableNameCaseSensitivity;
import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.relational.Attribute;
import io.debezium.relational.Column;
import io.debezium.relational.DefaultValueConverter;
import io.debezium.relational.HistorizedRelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.Tables;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.LRUCacheMap;

import oracle.jdbc.OracleTypes;

/**
 * The schema of an Oracle database.
 *
 * @author Gunnar Morling
 */
public class OracleDatabaseSchema extends HistorizedRelationalDatabaseSchema {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleDatabaseSchema.class);

    public static final String ATTRIBUTE_OBJECT_ID = "OBJECT_ID";
    public static final String ATTRIBUTE_DATA_OBJECT_ID = "DATA_OBJECT_ID";

    private final OracleDdlParser ddlParser;
    private final ConcurrentMap<TableId, List<Column>> lobColumnsByTableId = new ConcurrentHashMap<>();
    private final OracleValueConverters valueConverters;
    private final LRUCacheMap<Long, TableId> objectIdToTableId;

    public OracleDatabaseSchema(OracleConnectorConfig connectorConfig, OracleValueConverters valueConverters,
                                DefaultValueConverter defaultValueConverter, SchemaNameAdjuster schemaNameAdjuster,
                                TopicNamingStrategy<TableId> topicNamingStrategy, TableNameCaseSensitivity tableNameCaseSensitivity) {
        super(connectorConfig, topicNamingStrategy, connectorConfig.getTableFilters().dataCollectionFilter(),
                connectorConfig.getColumnFilter(),
                new TableSchemaBuilder(
                        valueConverters,
                        defaultValueConverter,
                        schemaNameAdjuster,
                        connectorConfig.customConverterRegistry(),
                        connectorConfig.getSourceInfoStructMaker().schema(),
                        connectorConfig.getFieldNamer(),
                        false),
                TableNameCaseSensitivity.INSENSITIVE.equals(tableNameCaseSensitivity),
                connectorConfig.getKeyMapper());

        this.valueConverters = valueConverters;
        this.ddlParser = new OracleDdlParser(
                true,
                false,
                connectorConfig.isSchemaCommentsHistoryEnabled(),
                valueConverters,
                connectorConfig.getTableFilters().dataCollectionFilter());

        this.objectIdToTableId = new LRUCacheMap<>(connectorConfig.getObjectIdToTableIdCacheSize());
    }

    public Tables getTables() {
        return tables();
    }

    public OracleValueConverters getValueConverters() {
        return valueConverters;
    }

    @Override
    public OracleDdlParser getDdlParser() {
        return ddlParser;
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChange) {
        LOGGER.debug("Applying schema change event {}", schemaChange);

        switch (schemaChange.getType()) {
            case CREATE:
            case ALTER:
                schemaChange.getTableChanges().forEach(x -> {
                    buildAndRegisterSchema(x.getTable());
                    tables().overwriteTable(x.getTable());
                });
                break;
            case DROP:
                schemaChange.getTableChanges().forEach(x -> removeSchema(x.getId()));
                break;
            default:
        }

        if (!storeOnlyCapturedTables() ||
                schemaChange.getTables().stream().map(Table::id).anyMatch(getTableFilter()::isIncluded)) {
            LOGGER.debug("Recorded DDL statements for database '{}': {}", schemaChange.getDatabase(), schemaChange.getDdl());
            record(schemaChange, schemaChange.getTableChanges());
        }
    }

    @Override
    protected void removeSchema(TableId id) {
        super.removeSchema(id);
        lobColumnsByTableId.remove(id);
    }

    @Override
    protected void buildAndRegisterSchema(Table table) {
        if (getTableFilter().isIncluded(table.id())) {
            super.buildAndRegisterSchema(table);

            // Cache LOB column mappings for performance
            buildAndRegisterTableLobColumns(table);

            // Cache Object ID to Table ID for performance
            buildAndRegisterTableObjectIdReferences(table);
        }
    }

    /**
     * Get the {@link TableId} by {@code objectId}.
     *
     * @param objectId the object id find the table id about, cannot be {@code null}
     * @param dataObjectId the data object id, may be {@code null}
     * @return the table identifier or {@code null} if no entry is cached or in the schema with the object id
     */
    public TableId getTableIdByObjectId(Long objectId, Long dataObjectId) {
        Objects.requireNonNull(objectId, "The database table object id is null and is not allowed");
        // Internally we cache this using a bounded cache for performance reasons, particularly when a
        // transaction may refer to the same table for consecutive DML events. This avoids the need to
        // iterate the list of tables on each DML event observed.
        return objectIdToTableId.computeIfAbsent(objectId, (tableObjectId) -> {
            for (TableId tableId : tableIds()) {
                final Table table = tableFor(tableId);
                final Attribute attribute = table.attributeWithName(ATTRIBUTE_OBJECT_ID);
                if (attribute != null && attribute.asLong().equals(tableObjectId)) {
                    if (dataObjectId != null) {
                        final Attribute dataAttribute = table.attributeWithName(ATTRIBUTE_DATA_OBJECT_ID);
                        if (dataAttribute == null || !dataAttribute.asLong().equals(dataObjectId)) {
                            // Did not match, continue
                            continue;
                        }
                    }
                    LOGGER.debug("Table lookup for object {} resolved to '{}'", tableObjectId, table.id());
                    return table.id();
                }
            }
            LOGGER.debug("Table lookup for object id {} did not find a match.", tableObjectId);
            return null;
        });
    }

    /**
     * Get a list of large object (LOB) columns for the specified relational table identifier.
     *
     * @param id the relational table identifier
     * @return a list of LOB columns, may be empty if the table has no LOB columns
     */
    public List<Column> getLobColumnsForTable(TableId id) {
        return lobColumnsByTableId.getOrDefault(id, Collections.emptyList());
    }

    /**
     * Returns whether the specified value is the unavailable value placeholder for an LOB column.
     */
    public boolean isColumnUnavailableValuePlaceholder(Column column, Object value) {
        if (isClobColumn(column) || isXmlColumn(column)) {
            return valueConverters.getUnavailableValuePlaceholderString().equals(value);
        }
        else if (isBlobColumn(column)) {
            return ByteBuffer.wrap(valueConverters.getUnavailableValuePlaceholderBinary()).equals(value);
        }
        return false;
    }

    /**
     * Return whether the provided relational column model is a LOB data type.
     */
    public static boolean isLobColumn(Column column) {
        return isClobColumn(column) || isBlobColumn(column);
    }

    /**
     * Return whether the provided relational column model is a XML data type.
     */
    public static boolean isXmlColumn(Column column) {
        return column.jdbcType() == OracleTypes.SQLXML;
    }

    /**
     * Returns whether the provided relational column model is a CLOB or NCLOB data type.
     */
    private static boolean isClobColumn(Column column) {
        return column.jdbcType() == OracleTypes.CLOB || column.jdbcType() == OracleTypes.NCLOB;
    }

    /**
     * Returns whether the provided relational column model is a CLOB data type.
     */
    private static boolean isBlobColumn(Column column) {
        return column.jdbcType() == OracleTypes.BLOB;
    }

    private void buildAndRegisterTableObjectIdReferences(Table table) {
        final Attribute attribute = table.attributeWithName(ATTRIBUTE_OBJECT_ID);
        if (attribute != null) {
            objectIdToTableId.put(attribute.asLong(), table.id());
        }
    }

    private void buildAndRegisterTableLobColumns(Table table) {
        final List<Column> lobColumns = new ArrayList<>();
        for (Column column : table.columns()) {
            switch (column.jdbcType()) {
                case OracleTypes.CLOB:
                case OracleTypes.NCLOB:
                case OracleTypes.BLOB:
                case OracleTypes.SQLXML:
                    lobColumns.add(column);
                    break;
            }
        }
        if (!lobColumns.isEmpty()) {
            lobColumnsByTableId.put(table.id(), lobColumns);
        }
        else {
            lobColumnsByTableId.remove(table.id());
        }
    }
}
