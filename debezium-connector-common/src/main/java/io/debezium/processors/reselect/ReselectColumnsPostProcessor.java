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
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.bean.StandardBeanNames;
import io.debezium.bean.spi.BeanRegistry;
import io.debezium.bean.spi.BeanRegistryAware;
import io.debezium.common.annotation.Incubating;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.config.Instantiator;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Envelope;
import io.debezium.data.Json;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.function.Predicates;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.processors.reselect.cache.MemoryReselectColumnCache;
import io.debezium.processors.reselect.cache.ReselectColumnCache;
import io.debezium.processors.spi.PostProcessor;
import io.debezium.relational.Column;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.ValueConverter;
import io.debezium.relational.ValueConverterProvider;
import io.debezium.service.spi.ServiceRegistry;
import io.debezium.service.spi.ServiceRegistryAware;
import io.debezium.util.ByteBuffers;
import io.debezium.util.Strings;

/**
 * An implementation of the Debezium {@link PostProcessor} contract that allows for the re-selection of
 * columns that are populated with the unavailable value placeholder or that the user wishes to have
 * re-queried with the latest state if the column's value happens to be {@code null}.
 *
 * @author Chris Cranford
 */
@Incubating
public class ReselectColumnsPostProcessor implements PostProcessor, BeanRegistryAware, ServiceRegistryAware {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReselectColumnsPostProcessor.class);

    private static final String RESELECT_COLUMNS_INCLUDE_LIST = "reselect.columns.include.list";
    private static final String RESELECT_COLUMNS_EXCLUDE_LIST = "reselect.columns.exclude.list";
    private static final String RESELECT_UNAVAILABLE_VALUES = "reselect.unavailable.values";
    private static final String RESELECT_NULL_VALUES = "reselect.null.values";
    private static final String RESELECT_USE_EVENT_KEY = "reselect.use.event.key";

    // Optional caching of re-selected values. Caching is OFF by default: re-selection exists to fetch the
    // latest committed row state, so caching trades freshness for fewer database round-trips. It is most
    // useful when the same rows are re-selected repeatedly (e.g. TOAST/LOB columns re-queried on every
    // unrelated update). The cache is invalidated on modify (see apply()), so correctness does not depend
    // on the cache's TTL.
    //
    // The cache is pluggable via a strategy: 'reselect.cache.enabled' turns it on and 'reselect.cache.type'
    // selects the implementation class (defaulting to an in-memory cache). Alternative implementations
    // (e.g. an embedded key/value store) can be supplied without changing this post-processor.
    private static final String RESELECT_CACHE_ENABLED = "reselect.cache.enabled";
    private static final String RESELECT_CACHE_TYPE = "reselect.cache.type";

    private static final boolean DEFAULT_RESELECT_CACHE_ENABLED = false;
    private static final String DEFAULT_RESELECT_CACHE_TYPE = MemoryReselectColumnCache.class.getName();

    private Predicate<String> selector;
    private boolean reselectUnavailableValues;
    private boolean reselectNullValues;
    private boolean reselectUseEventKeyFields;
    private ReselectColumnCache reselectCache;
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
    private CustomConverterRegistry customConverterRegistry;

    public static final Field ERROR_HANDLING_MODE = Field.create("reselect.error.handling.mode")
            .withDisplayName("Error Handling")
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR))
            .withEnum(ErrorHandlingMode.class, ErrorHandlingMode.WARN)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Specify how to handle error in case of lookup sql failure or empty reselection: "
                    + "'warn' only log the error; "
                    + "'fail' fail the connector with an error message.");

    private ErrorHandlingMode errorHandlingMode;

    public enum ErrorHandlingMode implements EnumeratedValue {
        /**
         * Error handling to be used when doing lookup for the value.
         */
        WARN("warn"),
        FAIL("fail");

        private final String value;

        ErrorHandlingMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static ErrorHandlingMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (ErrorHandlingMode option : ErrorHandlingMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }
    }

    @Override
    public void configure(Map<String, ?> properties) {
        final Configuration config = Configuration.from(properties);
        this.reselectUnavailableValues = config.getBoolean(RESELECT_UNAVAILABLE_VALUES, true);
        this.reselectNullValues = config.getBoolean(RESELECT_NULL_VALUES, true);
        this.reselectUseEventKeyFields = config.getBoolean(RESELECT_USE_EVENT_KEY, false);
        this.errorHandlingMode = ErrorHandlingMode.parse(config.getString(ERROR_HANDLING_MODE));
        this.selector = new ReselectColumnsPredicateBuilder()
                .includeColumns(config.getString(RESELECT_COLUMNS_INCLUDE_LIST))
                .excludeColumns(config.getString(RESELECT_COLUMNS_EXCLUDE_LIST))
                .build();

        if (!(this.reselectNullValues || this.reselectUnavailableValues)) {
            LOGGER.warn("Reselect post-processor disables both null and unavailable columns, no-reselection will occur.");
        }

        if (config.getBoolean(RESELECT_CACHE_ENABLED, DEFAULT_RESELECT_CACHE_ENABLED)) {
            final String cacheType = config.getString(RESELECT_CACHE_TYPE, DEFAULT_RESELECT_CACHE_TYPE);
            this.reselectCache = Instantiator.getInstance(cacheType);
            this.reselectCache.configure(config);
            LOGGER.info("Reselect cache enabled using strategy '{}'.", cacheType);
        }
    }

    @Override
    public void close() {
        if (reselectCache != null) {
            reselectCache.close();
        }
    }

    private boolean isCacheEnabled() {
        return reselectCache != null;
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

        // Resolve the key columns/values once so cache reads, writes and invalidation all share the
        // exact same row identity (whether the event key or the primary key is used).
        final List<String> keyColumns = new ArrayList<>();
        final List<Object> keyValues = new ArrayList<>();
        if (reselectUseEventKeyFields) {
            for (org.apache.kafka.connect.data.Field field : key.schema().fields()) {
                keyColumns.add(field.name());
                keyValues.add(resolveKeyFieldValue(key, field));
            }
        }
        else {
            for (Column column : table.primaryKeyColumns()) {
                keyColumns.add(column.name());
                keyValues.add(resolveKeyFieldValue(after, after.schema().field(column.name())));
            }
        }

        final Set<String> requiredColumnSelections = getRequiredColumnSelections(tableId, after);

        // Resolve the row-scoped cache view once, keyed by the event's message key struct, so the row
        // identity is resolved a single time and reused for every column of this row. Using the key struct
        // means schema/DDL/default-value changes naturally produce a cache miss rather than a false hit.
        final ReselectColumnCache.RowCache rowCache = isCacheEnabled() ? reselectCache.forRow(key) : null;

        // Cache-on-modify: any column that arrived with a real (non-placeholder) value reflects the row's
        // current state, so refresh the cache with it. A later event that does not modify that column
        // (e.g. an unchanged TOAST/LOB re-emitted as a placeholder) can then be served from the cache
        // instead of re-querying. This also keeps the cache correct across row modifications without
        // depending on the TTL.
        if (isCacheEnabled()) {
            cacheModifiedColumns(rowCache, tableId, after, requiredColumnSelections);
        }

        if (requiredColumnSelections.isEmpty()) {
            LOGGER.debug("No columns require re-selection.");
            return;
        }

        // Per-column cache lookup. Each required column is cached independently under this row, so events
        // touching different placeholder subsets of the same row reuse each other's results. Cached
        // values are the final converted values, so a hit is applied to the event directly. A hit may
        // carry a null value, distinguished from a miss by the Hit holder.
        final Map<String, Object> selections = new HashMap<>();
        final List<String> columnsToQuery = new ArrayList<>();
        if (rowCache != null) {
            for (String columnName : requiredColumnSelections) {
                final Optional<ReselectColumnCache.Hit> cached = rowCache.get(columnName);
                if (cached.isPresent()) {
                    selections.put(columnName, cached.get().value());
                }
                else {
                    columnsToQuery.add(columnName);
                }
            }
        }
        else {
            columnsToQuery.addAll(requiredColumnSelections);
        }

        if (!columnsToQuery.isEmpty()) {
            final Map<String, Object> rawValues = new HashMap<>();
            try {
                final boolean found = jdbcConnection.reselectColumns(table, columnsToQuery, keyColumns, keyValues, source, rs -> {
                    for (String columnName : columnsToQuery) {
                        rawValues.put(columnName, rs.getObject(columnName));
                    }
                });
                if (!found) {
                    if (errorHandlingMode == ErrorHandlingMode.FAIL) {
                        throw new DebeziumException("Failed to find row in table " + tableId + " with key " + key);
                    }
                    LOGGER.warn("Failed to find row in table {} with key {}.", tableId, key);
                    return;
                }
            }
            catch (SQLException e) {
                if (errorHandlingMode == ErrorHandlingMode.FAIL) {
                    throw new DebeziumException("Failed to re-select columns for table " + tableId + " and key " + keyValues, e);
                }
                LOGGER.warn("Failed to re-select columns for table {} and key {}", tableId, keyValues, e);
                return;
            }

            // Convert freshly queried raw values once, then both apply and cache the converted value so
            // cache hits and freshly queried values are handled identically.
            for (String columnName : columnsToQuery) {
                if (!rawValues.containsKey(columnName)) {
                    continue;
                }
                final Column column = table.columnWithName(columnName);
                final org.apache.kafka.connect.data.Field field = after.schema().field(columnName);
                final Object convertedValue = getConvertedValue(tableId, column, field, rawValues.get(columnName));
                selections.put(columnName, convertedValue);
                if (rowCache != null) {
                    rowCache.put(columnName, convertedValue);
                }
            }
        }

        // Iterate re-selection columns and override placeholder values with the re-selected (cached or
        // freshly queried) converted values.
        for (String columnName : requiredColumnSelections) {
            if (!selections.containsKey(columnName)) {
                continue;
            }
            final org.apache.kafka.connect.data.Field field = after.schema().field(columnName);
            final Object convertedValue = selections.get(columnName);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Replaced field {} value {} with {}", field.name(), value.get(field), convertedValue);
            }
            after.put(field.name(), convertedValue);
        }
    }

    /**
     * Refresh the cache with columns in this event that carry a real value (i.e. are not placeholders and
     * therefore are not being re-selected). Caching the current value lets a subsequent event that does
     * not modify the column be served from the cache, and keeps the cache correct across modifications so
     * a long TTL never serves a value that has since changed. Only columns eligible for re-selection (per
     * the column selector) are cached, so unrelated columns do not bloat the cache.
     *
     * @param requiredColumnSelections columns being re-selected this event, which are skipped here since
     *                                 they will be cached after re-selection below.
     */
    private void cacheModifiedColumns(ReselectColumnCache.RowCache rowCache, TableId tableId, Struct after, Set<String> requiredColumnSelections) {
        if (rowCache == null) {
            return;
        }
        for (org.apache.kafka.connect.data.Field field : after.schema().fields()) {
            final String columnName = field.name();
            if (requiredColumnSelections.contains(columnName)) {
                continue;
            }
            final String fullyQualifiedName = jdbcConnection.getQualifiedTableName(tableId) + ":" + columnName;
            if (selector.test(fullyQualifiedName)) {
                rowCache.put(columnName, after.get(field));
            }
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
        this.jdbcConnection = beanRegistry.lookupByName(StandardBeanNames.JDBC_CONNECTION, JdbcConnection.class);
        this.schema = beanRegistry.lookupByName(StandardBeanNames.DATABASE_SCHEMA, RelationalDatabaseSchema.class);
    }

    @Override
    public void injectServiceRegistry(ServiceRegistry serviceRegistry) {
        this.customConverterRegistry = serviceRegistry.tryGetService(CustomConverterRegistry.class);
    }

    private Object resolveKeyFieldValue(Struct key, org.apache.kafka.connect.data.Field field) {
        if (field.schema() != null && VariableScaleDecimal.LOGICAL_NAME.equals(field.schema().name())) {
            final Struct value = key.getStruct(field.name());
            if (value != null) {
                final SpecialValueDecimal decimal = VariableScaleDecimal.toLogical(key.getStruct(field.name()));
                return decimal.getWrappedValue();
            }
        }
        return key.get(field);
    }

    private Set<String> getRequiredColumnSelections(TableId tableId, Struct after) {
        final Set<String> columnSelections = new LinkedHashSet<>();
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
                if (value instanceof byte[] valueArray) {
                    return ByteBuffers.equals(unavailableValuePlaceholderBytes, valueArray);
                }
                else if (value instanceof ByteBuffer valueBuffer) {
                    return unavailableValuePlaceholderBytes.equals(valueBuffer);
                }
                return false;
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

    private Object getConvertedValue(TableId tableId, Column column, org.apache.kafka.connect.data.Field field, Object value) {
        final ValueConverter converter = customConverterRegistry.getValueConverter(tableId, column)
                .orElse(valueConverterProvider.converter(column, field));
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
