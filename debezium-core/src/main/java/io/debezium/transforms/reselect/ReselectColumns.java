/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.reselect;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.common.annotation.Incubating;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Envelope;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.transforms.SmtManager;
import io.debezium.util.Strings;

/**
 * An abstract single message transformation that is capable of re-selection of columns.
 *
 * When a column is populated with the {@link RelationalDatabaseConnectorConfig#UNAVAILABLE_VALUE_PLACEHOLDER}
 * value, this represents a value that wasn't changed nor was available in the change event. This will trigger
 * this transformation to attempt to re-select the value from the target table and replace the unavailable
 * value with the value currently in the database.
 *
 * Additionally, the user can specify a comma-separated list of columns that when the value of the column is
 * {@code null}, the transformation will also re-query the target table and populate the field with the
 * value currently in the database. This is useful for some connectors that may treat specific table columns
 * like an unavailable value placeholder scenario but there is insufficient metadata at the JDBC level
 * for Debezium to make sure determinations, such as Oracle Exadata extended strings.
 *
 * A specific connector should extend this class with its own implementation details
 *
 * @author Chris Cranford
 */
@Incubating
public class ReselectColumns<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReselectColumns.class);

    private static final Field COLUMN_LIST = Field.create("column.list")
            .withDisplayName("List of columns to reselect")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .required()
            .withDescription("Comma-separated list of columns to be reselected from source database");

    private static final Field CONNECTION_URL = Field.create("connection.url")
            .withDisplayName("JDBC connection url")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .required()
            .withDescription("JDBC connection URL");

    private static final Field CONNECTION_USER = Field.create("connection.user")
            .withDisplayName("JDBC connection username")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .required()
            .withDescription("JDBC connection username");

    private static final Field CONNECTION_PASSWORD = Field.create("connection.password")
            .withDisplayName("JDBC connection password")
            .withType(ConfigDef.Type.PASSWORD)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .required()
            .withDescription("JDBC connection password");

    private static final Field UNAVAILABLE_VALUE_PLACEHOLDER = RelationalDatabaseConnectorConfig.UNAVAILABLE_VALUE_PLACEHOLDER;

    protected SmtManager<R> smtManager;
    protected List<Pattern> columnsList;
    protected String placeholder;
    protected ByteBuffer placeholderBytes;
    protected Connection connection;
    protected HashMap<String, ReselectColumnsMetadataProvider<R>> metadataProviders = new HashMap<>();

    @Override
    public ConfigDef config() {
        final ConfigDef config = new ConfigDef();
        Field.group(config, null, COLUMN_LIST, CONNECTION_URL, CONNECTION_USER, CONNECTION_PASSWORD);
        return config;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> configs) {
        final Configuration config = Configuration.from(configs);
        this.smtManager = new SmtManager<>(config);
        this.columnsList = Strings.listOfRegex(config.getString(COLUMN_LIST), Pattern.CASE_INSENSITIVE);

        this.placeholder = config.getString(UNAVAILABLE_VALUE_PLACEHOLDER);
        this.placeholderBytes = ByteBuffer.wrap(this.placeholder.getBytes(StandardCharsets.UTF_8));

        try {
            Properties properties = new Properties();
            properties.put("user", config.getString(CONNECTION_USER));
            properties.put("password", config.getString(CONNECTION_PASSWORD));
            this.connection = DriverManager.getConnection(config.getString(CONNECTION_URL), properties);
        }
        catch (SQLException e) {
            throw new ConnectException("Failed to connect to database", e);
        }

        for (ReselectColumnsMetadataProvider<R> provider : ServiceLoader.load(ReselectColumnsMetadataProvider.class)) {
            metadataProviders.put(provider.getName(), provider);
        }
    }

    @Override
    public void close() {
        if (connection != null) {
            try {
                connection.close();
            }
            catch (SQLException e) {
                throw new ConnectException("Failed to close database connection.", e);
            }
        }
    }

    @Override
    public R apply(R record) {
        if (!smtManager.isValidEnvelope(record)) {
            LOGGER.debug("Not a valid envelope: {}", record);
            return record;
        }

        final Object value = record.value();
        if (value == null) {
            LOGGER.debug("Record has no value, no re-selection possible.");
            return record;
        }
        else if (!(value instanceof Struct)) {
            LOGGER.debug("Value must be of Struct for re-reselection");
            return record;
        }

        final Struct after = ((Struct) value).getStruct(Envelope.FieldName.AFTER);
        if (after == null) {
            LOGGER.debug("Incoming record has no after block {}", record);
            return record;
        }

        final Struct source = ((Struct) value).getStruct(Envelope.FieldName.SOURCE);
        if (source == null) {
            LOGGER.debug("Incoming record has an empty source info block {}", record);
            return record;
        }

        final List<String> reselectColumns = resolveReselectColumns(after);
        if (reselectColumns.isEmpty()) {
            LOGGER.debug("No columns require reselect.");
            return record;
        }

        final TableId tableId = getTableIdFromSource(source);
        if (tableId == null) {
            LOGGER.warn("Failed to resolve table id from source info block {}", record);
            return record;
        }

        final ReselectColumnsMetadataProvider<R> metadataProvider = resolveQueryProvider(source);
        if (metadataProvider == null) {
            LOGGER.warn("Failed to resolve metadata re-select provider {}", record);
            return record;
        }

        final String reselectQuery = metadataProvider.getQuery(record, source, reselectColumns, tableId);
        if (Strings.isNullOrEmpty(reselectQuery)) {
            LOGGER.warn("Failed to generate re-select query for record {}", record);
            return record;
        }

        final Struct newValue = createNewValueFromOld(record, (Struct) value);
        try {
            populateNewValueFromReselect(reselectQuery, reselectColumns, newValue, metadataProvider);
        }
        catch (Exception e) {
            LOGGER.error("Failed to re-select data into the outbound event", e);
            return record;
        }

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                newValue,
                record.timestamp());
    }

    protected List<String> resolveReselectColumns(Struct after) {
        final List<String> columns = new ArrayList<>();
        for (org.apache.kafka.connect.data.Field field : after.schema().fields()) {
            final Object value = after.getWithoutDefault(field.name());
            if (isFieldValueUnavailablePlaceholder(field, value)) {
                LOGGER.debug("Field {} scheduled for re-selection, value is placeholder.", field.name());
                columns.add(field.name());
            }
            else if (value == null) {
                for (Pattern pattern : columnsList) {
                    final Matcher matcher = pattern.matcher(field.name());
                    if (matcher.matches()) {
                        LOGGER.debug("Field {} scheduled for re-selection, value is null.", field.name());
                        columns.add(field.name());
                    }
                }
            }
        }
        return columns;
    }

    protected ReselectColumnsMetadataProvider<R> resolveQueryProvider(Struct source) {
        final String connectorName = source.getString(AbstractSourceInfo.DEBEZIUM_CONNECTOR_KEY);
        if (!Strings.isNullOrEmpty(connectorName)) {
            ReselectColumnsMetadataProvider<R> provider = metadataProviders.get(connectorName);
            if (provider != null) {
                return provider;
            }
        }
        return metadataProviders.get("default");
    }

    protected boolean isFieldValueUnavailablePlaceholder(org.apache.kafka.connect.data.Field field, Object value) {
        if (field.schema().type() == Schema.Type.BYTES) {
            return placeholderBytes.equals(value);
        }
        return placeholder.equals(value);
    }

    protected Struct createNewValueFromOld(R record, Struct oldValue) {
        final Struct newValue = new Struct(record.valueSchema());
        for (org.apache.kafka.connect.data.Field field : record.valueSchema().fields()) {
            newValue.put(field.name(), oldValue.get(field));
        }
        return newValue;
    }

    protected void populateNewValueFromReselect(String query, List<String> columns, Struct value, ReselectColumnsMetadataProvider<R> metadataProvider)
            throws SQLException {
        final Struct after = value.getStruct(Envelope.FieldName.AFTER);
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    for (int i = 0; i < columns.size(); i++) {
                        final String columnName = columns.get(i);
                        final int jdbcType = rs.getMetaData().getColumnType(i + 1);
                        final Object columnValue = rs.getObject(columnName);
                        after.put(columnName, metadataProvider.convertValue(jdbcType, columnValue));
                    }
                }
            }
        }
    }

    protected TableId getTableIdFromSource(Struct source) {
        final String databaseName = source.getString(AbstractSourceInfo.DATABASE_NAME_KEY);
        final String schemaName = source.getString(AbstractSourceInfo.SCHEMA_NAME_KEY);
        final String tableName = source.getString(AbstractSourceInfo.TABLE_NAME_KEY);

        if (databaseName == null || schemaName == null || tableName == null) {
            LOGGER.debug("Incoming record has an empty database '{}' or schema '{}' or table '{}' name", databaseName, schemaName, tableName);
            return null;
        }

        return new TableId(databaseName, schemaName, tableName);
    }

}
