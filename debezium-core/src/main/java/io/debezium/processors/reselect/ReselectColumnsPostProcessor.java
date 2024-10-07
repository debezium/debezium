/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.processors.reselect;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.bean.StandardBeanNames;
import io.debezium.bean.spi.BeanRegistry;
import io.debezium.bean.spi.BeanRegistryAware;
import io.debezium.common.annotation.Incubating;
import io.debezium.config.Configuration;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Envelope;
import io.debezium.data.Json;
import io.debezium.function.Predicates;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.processors.spi.PostProcessor;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.ValueConverter;
import io.debezium.relational.ValueConverterProvider;
import io.debezium.util.Strings;

/**
 * An implementation of the Debezium {@link PostProcessor} contract that allows for the re-selection of
 * columns that are populated with the unavailable value placeholder or that the user wishes to have
 * re-queried with the latest state if the column's value happens to be {@code null}.
 *
 * @author Chris Cranford
 */
@Incubating
public class ReselectColumnsPostProcessor implements PostProcessor, BeanRegistryAware {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReselectColumnsPostProcessor.class);

    private static final String RESELECT_COLUMNS_INCLUDE_LIST = "reselect.columns.include.list";
    private static final String RESELECT_COLUMNS_EXCLUDE_LIST = "reselect.columns.exclude.list";
    private static final String RESELECT_UNAVAILABLE_VALUES = "reselect.unavailable.values";
    private static final String RESELECT_NULL_VALUES = "reselect.null.values";
    private static final String RESELECT_USE_EVENT_KEY = "reselect.use.event.key";

    private Predicate<String> selector;
    private boolean reselectUnavailableValues;
    private boolean reselectNullValues;
    private boolean reselectUseEventKeyFields;
    private JdbcConnection jdbcConnection;
    private ValueConverterProvider valueConverterProvider;
    private String unavailableValuePlaceholder;
    private ByteBuffer unavailableValuePlaceholderBytes;
    private Map<String, String> unavailableValuePlaceholderMap;
    private String unavailableValuePlaceholderJson;
    private List<Integer> unavailablePlaceholderIntArray;
    private List<Long> unavailablePlaceholderLongArray;
    private RelationalDatabaseSchema schema;
    private RelationalDatabaseConnectorConfig connectorConfig;

    @Override
    public void configure(Map<String, ?> properties) {
        final Configuration config = Configuration.from(properties);
        this.reselectUnavailableValues = config.getBoolean(RESELECT_UNAVAILABLE_VALUES, true);
        this.reselectNullValues = config.getBoolean(RESELECT_NULL_VALUES, true);
        this.reselectUseEventKeyFields = config.getBoolean(RESELECT_USE_EVENT_KEY, false);
        this.selector = new ReselectColumnsPredicateBuilder()
                .includeColumns(config.getString(RESELECT_COLUMNS_INCLUDE_LIST))
                .excludeColumns(config.getString(RESELECT_COLUMNS_EXCLUDE_LIST))
                .build();

        if (!(this.reselectNullValues || this.reselectUnavailableValues)) {
            LOGGER.warn("Reselect post-processor disables both null and unavailable columns, no-reselection will occur.");
        }
    }

    @Override
    public void close() {
        // nothing to do
    }

    protected JdbcConnection getJdbcConnection() {
        return this.jdbcConnection;
    }

    public void apply(Object messageKey, Struct value) {
        if (value == null) {
            LOGGER.debug("Value is not a Struct, no re-selection possible.");
            return;
        }

        if (!(messageKey instanceof Struct)) {
            LOGGER.debug("Key is not a Struct, no re-selection possible.");
            return;
        }

        final Struct key = (Struct) messageKey;

        final Struct after = value.getStruct(Envelope.FieldName.AFTER);
        if (after == null) {
            LOGGER.debug("Value has no after field, no re-selection possible.");
            return;
        }

        // Skip read events as these are generated from raw JDBC selects which should have the current
        // state of the row and there is no reason to logically re-select the column state.
        final String operation = value.getString(Envelope.FieldName.OPERATION);
        if (Envelope.Operation.READ.code().equals(operation)) {
            return;
        }

        final Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        if (source == null) {
            LOGGER.debug("Value has no source field, no re-selection possible.");
            return;
        }

        final TableId tableId = getTableIdFromSource(source);
        if (tableId == null) {
            return;
        }

        if (connectorConfig.isSignalDataCollection(tableId)) {
            LOGGER.debug("Signal table '{}' events are not eligible for re-selection.", tableId);
            return;
        }

        final Table table = schema.tableFor(tableId);
        if (table == null) {
            LOGGER.debug("Unable to locate table {} in relational model.", tableId);
            return;
        }

        final List<String> requiredColumnSelections = getRequiredColumnSelections(tableId, after);
        if (requiredColumnSelections.isEmpty()) {
            LOGGER.debug("No columns require re-selection.");
            return;
        }

        final List<String> keyColumns = new ArrayList<>();
        final List<Object> keyValues = new ArrayList<>();
        if (reselectUseEventKeyFields) {
            for (org.apache.kafka.connect.data.Field field : key.schema().fields()) {
                keyColumns.add(field.name());
                keyValues.add(key.get(field));
            }
        }
        else {
            for (Column column : table.primaryKeyColumns()) {
                keyColumns.add(column.name());
                keyValues.add(after.get(after.schema().field(column.name())));
            }
        }

        final Map<String, Object> selections;
        try {
            selections = jdbcConnection.reselectColumns(tableId, requiredColumnSelections, keyColumns, keyValues, source);
            if (selections.isEmpty()) {
                LOGGER.warn("Failed to find row in table {} with key {}.", tableId, key);
                return;
            }
        }
        catch (SQLException e) {
            LOGGER.warn("Failed to re-select row for table {} and key {}", tableId, key, e);
            return;
        }

        // Iterate re-selection columns and override old values
        for (Map.Entry<String, Object> selection : selections.entrySet()) {
            final String columnName = selection.getKey();
            final Column column = table.columnWithName(columnName);
            final org.apache.kafka.connect.data.Field field = after.schema().field(columnName);

            final Object convertedValue = getConvertedValue(column, field, selection.getValue());
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Replaced field {} value {} with {}", field.name(), value.get(field), convertedValue);
            }
            after.put(field.name(), convertedValue);
        }
    }

    @Override
    public void injectBeanRegistry(BeanRegistry beanRegistry) {
        this.connectorConfig = beanRegistry.lookupByName(StandardBeanNames.CONNECTOR_CONFIG, RelationalDatabaseConnectorConfig.class);

        // Various unavailable value placeholders
        this.unavailableValuePlaceholder = new String(connectorConfig.getUnavailableValuePlaceholder());
        this.unavailableValuePlaceholderBytes = ByteBuffer.wrap(connectorConfig.getUnavailableValuePlaceholder());
        this.unavailableValuePlaceholderMap = Map.of(this.unavailableValuePlaceholder, this.unavailableValuePlaceholder);
        this.unavailableValuePlaceholderJson = "{\"" + this.unavailableValuePlaceholder + "\":\"" + this.unavailableValuePlaceholder + "\"}";
        unavailablePlaceholderIntArray = new ArrayList<>(unavailableValuePlaceholderBytes.limit());
        unavailablePlaceholderLongArray = new ArrayList<>(unavailableValuePlaceholderBytes.limit());
        for (byte b : unavailableValuePlaceholderBytes.array()) {
            unavailablePlaceholderIntArray.add((int) b);
            unavailablePlaceholderLongArray.add((long) b);
        }

        this.valueConverterProvider = beanRegistry.lookupByName(StandardBeanNames.VALUE_CONVERTER, ValueConverterProvider.class);
        this.jdbcConnection = resolveJdbcConnection(beanRegistry);
        this.schema = beanRegistry.lookupByName(StandardBeanNames.DATABASE_SCHEMA, RelationalDatabaseSchema.class);
    }

    private List<String> getRequiredColumnSelections(TableId tableId, Struct after) {
        final List<String> columnSelections = new ArrayList<>();
        for (org.apache.kafka.connect.data.Field field : after.schema().fields()) {
            final Object value = after.get(field);
            if (reselectUnavailableValues && isUnavailableValueHolder(field, value)) {
                final String fullyQualifiedName = jdbcConnection.getQualifiedTableName(tableId) + ":" + field.name();
                if (selector.test(fullyQualifiedName)) {
                    LOGGER.debug("Adding column {} for table {} to re-select list due to unavailable value placeholder.",
                            field.name(), tableId);
                    columnSelections.add(field.name());
                }
            }
            else if (reselectNullValues && value == null) {
                final String fullyQualifiedName = jdbcConnection.getQualifiedTableName(tableId) + ":" + field.name();
                if (selector.test(fullyQualifiedName)) {
                    LOGGER.debug("Adding empty column {} for table {} to re-select list.", field.name(), tableId);
                    columnSelections.add(field.name());
                }
            }
        }
        return columnSelections;
    }

    private boolean isUnavailableValueHolder(org.apache.kafka.connect.data.Field field, Object value) {
        if (unavailableValuePlaceholder != null) {
            if (field.schema().type() == Schema.Type.ARRAY && value != null) {
                // Special use case to inspect by element
                final Collection<?> values = (Collection<?>) value;
                for (Object collectionValue : values) {
                    if (isUnavailableValueHolder(field.schema().valueSchema(), collectionValue)) {
                        return true;
                    }
                }
                // Case for whole array value representing unavailable value
                return isUnavailableArrayValueHolder(field.schema(), value);
            }
            else {
                return isUnavailableValueHolder(field.schema(), value);
            }
        }
        return false;
    }

    private boolean isUnavailableValueHolder(Schema schema, Object value) {
        switch (schema.type()) {
            case BYTES:
                return unavailableValuePlaceholderBytes.equals(value);
            case MAP:
                return unavailableValuePlaceholderMap.equals(value);
            case STRING:
                // Both PostgreSQL HSTORE and JSON/JSONB have a schema name of "json".
                // PostgreSQL HSTORE fields use a JSON-like unavailable value placeholder, e.g., {"key":"value"},
                // while JSON/JSONB fields use a simple string placeholder.
                // This condition is needed to handle both cases:
                // - HSTORE unavailable value placeholders (as JSON objects)
                // - JSON/JSONB unavailable value placeholders (as strings)
                final boolean isJsonAndUnavailable = Json.LOGICAL_NAME.equals(schema.name()) && unavailableValuePlaceholderJson.equals(value);
                return unavailableValuePlaceholder.equals(value) || isJsonAndUnavailable;
        }
        return false;
    }

    private boolean isUnavailableArrayValueHolder(Schema schema, Object value) {
        assert schema.type() == Type.ARRAY;
        switch (schema.valueSchema().type()) {
            case INT32:
                return unavailablePlaceholderIntArray.equals(value);
            case INT64:
                return unavailablePlaceholderLongArray.equals(value);
            default:
                return false;
        }
    }

    private Object getConvertedValue(Column column, org.apache.kafka.connect.data.Field field, Object value) {
        final ValueConverter converter = valueConverterProvider.converter(column, field);
        if (converter != null) {
            return converter.convert(value);
        }
        return value;
    }

    private TableId getTableIdFromSource(Struct source) {
        final String databaseName = source.getString(AbstractSourceInfo.DATABASE_NAME_KEY);
        if (Strings.isNullOrEmpty(databaseName)) {
            LOGGER.debug("Database name is not available, no re-selection possible.");
            return null;
        }

        final String tableName = source.getString(AbstractSourceInfo.TABLE_NAME_KEY);
        if (Strings.isNullOrEmpty(tableName)) {
            LOGGER.debug("Table name is not available, no re-selection possible.");
            return null;
        }

        // Schema name can be optional in the case of certain connectors
        String schemaName = null;
        if (source.schema().field(AbstractSourceInfo.SCHEMA_NAME_KEY) != null) {
            schemaName = source.getString(AbstractSourceInfo.SCHEMA_NAME_KEY);
        }

        return jdbcConnection.createTableId(databaseName, schemaName, tableName);
    }

    protected JdbcConnection resolveJdbcConnection(BeanRegistry beanRegistry) {
        return beanRegistry.lookupByName(StandardBeanNames.JDBC_CONNECTION, JdbcConnection.class);
    }

    private static class ReselectColumnsPredicateBuilder {

        private Predicate<String> reselectColumnInclusions;
        private Predicate<String> reselectColumnExclusions;

        public ReselectColumnsPredicateBuilder includeColumns(String columnNames) {
            if (columnNames == null || columnNames.trim().isEmpty()) {
                reselectColumnInclusions = null;
            }
            else {
                reselectColumnInclusions = Predicates.includes(columnNames, Pattern.CASE_INSENSITIVE);
            }
            return this;
        }

        public ReselectColumnsPredicateBuilder excludeColumns(String columnNames) {
            if (columnNames == null || columnNames.trim().isEmpty()) {
                reselectColumnExclusions = null;
            }
            else {
                reselectColumnExclusions = Predicates.excludes(columnNames, Pattern.CASE_INSENSITIVE);
            }
            return this;
        }

        public Predicate<String> build() {
            if (reselectColumnInclusions != null) {
                return reselectColumnInclusions;
            }
            if (reselectColumnExclusions != null) {
                return reselectColumnExclusions;
            }
            return (x) -> true;
        }
    }

}
