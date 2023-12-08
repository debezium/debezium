/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.processors.reselect;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.bean.StandardBeanNames;
import io.debezium.bean.spi.BeanRegistry;
import io.debezium.common.annotation.Incubating;
import io.debezium.config.Configuration;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Envelope;
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
import io.debezium.service.spi.ServiceRegistry;
import io.debezium.service.spi.ServiceRegistryAware;
import io.debezium.util.Strings;

/**
 * An implementation of the Debezium {@link PostProcessor} contract that allows for the re-selection of
 * columns that are populated with the unavailable value placeholder or that the user wishes to have
 * re-queried with the latest state if the column's value happens to be {@code null}.
 *
 * This post-processor also implements a variety of injection-aware contracts to have the necessary
 * Debezium internal components provided at runtime so that various steps can be taken by this
 * post-processor.
 *
 * @author Chris Cranford
 */
@Incubating
public class ReselectColumnsPostProcessor implements PostProcessor, ServiceRegistryAware {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReselectColumnsPostProcessor.class);

    private static final String RESELECT_COLUMNS_LIST = "columns.list";

    private Predicate<String> selector = x -> true;
    private JdbcConnection jdbcConnection;
    private ValueConverterProvider valueConverterProvider;
    private String unavailableValuePlaceholder;
    private byte[] unavailableValuePlaceholderBytes;
    private RelationalDatabaseSchema schema;

    @Override
    public void configure(Map<String, ?> properties) {
        final Configuration config = Configuration.from(properties);
        if (config.hasKey(RESELECT_COLUMNS_LIST)) {
            final String reselectColumnNames = config.getString(RESELECT_COLUMNS_LIST);
            if (!Strings.isNullOrEmpty(reselectColumnNames)) {
                this.selector = Predicates.includes(reselectColumnNames, Pattern.CASE_INSENSITIVE);
            }
        }
    }

    @Override
    public void close() {
        // nothing to do
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

        final Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        if (source == null) {
            LOGGER.debug("Value has no source field, no re-selection possible.");
            return;
        }

        final TableId tableId = getTableIdFromSource(source);
        if (tableId == null) {
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
        for (org.apache.kafka.connect.data.Field field : key.schema().fields()) {
            keyColumns.add(field.name());
            keyValues.add(key.get(field));
        }

        Map<String, Object> selections = null;
        try {
            final String reselectQuery = jdbcConnection.buildReselectColumnQuery(tableId, requiredColumnSelections, keyColumns, source);
            selections = jdbcConnection.reselectColumns(reselectQuery, tableId, requiredColumnSelections, keyValues);
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
    public void injectServiceRegistry(ServiceRegistry serviceRegistry) {
        final BeanRegistry beanRegistry = serviceRegistry.getService(BeanRegistry.class);

        final RelationalDatabaseConnectorConfig connectorConfig = beanRegistry.lookupByName(
                StandardBeanNames.CONNECTOR_CONFIG, RelationalDatabaseConnectorConfig.class);
        this.unavailableValuePlaceholder = new String(connectorConfig.getUnavailableValuePlaceholder());
        this.unavailableValuePlaceholderBytes = connectorConfig.getUnavailableValuePlaceholder();

        this.valueConverterProvider = beanRegistry.lookupByName(StandardBeanNames.VALUE_CONVERTER, ValueConverterProvider.class);
        this.jdbcConnection = beanRegistry.lookupByName(StandardBeanNames.JDBC_CONNECTION, JdbcConnection.class);
        this.schema = beanRegistry.lookupByName(StandardBeanNames.DATABASE_SCHEMA, RelationalDatabaseSchema.class);
    }

    private List<String> getRequiredColumnSelections(TableId tableId, Struct after) {
        final List<String> columnSelections = new ArrayList<>();
        for (org.apache.kafka.connect.data.Field field : after.schema().fields()) {
            final Object value = after.get(field);
            if (isUnavailableValueHolder(field, value)) {
                LOGGER.debug("Adding column {} for table {} to re-select list due to unavailable value placeholder.",
                        field.name(), tableId);
                columnSelections.add(field.name());
            }
            else {
                final String fullyQualifiedName = jdbcConnection.getQualifiedTableName(tableId) + ":" + field.name();
                if (value == null && selector.test(fullyQualifiedName)) {
                    LOGGER.debug("Adding empty column {} for table {} to re-select list.", field.name(), tableId);
                    columnSelections.add(field.name());
                }
            }
        }
        return columnSelections;
    }

    private boolean isUnavailableValueHolder(org.apache.kafka.connect.data.Field field, Object value) {
        if (field.schema().type() == Schema.Type.BYTES && this.unavailableValuePlaceholderBytes != null) {
            return ByteBuffer.wrap(unavailableValuePlaceholderBytes).equals(value);
        }
        return unavailableValuePlaceholder != null && unavailableValuePlaceholder.equals(value);
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
}
