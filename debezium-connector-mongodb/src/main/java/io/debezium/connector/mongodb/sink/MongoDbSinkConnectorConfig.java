/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink;

import java.util.function.Consumer;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ConnectionString;

import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.mongodb.shared.SharedMongoDbConnectorConfig;
import io.debezium.sink.SinkConnectorConfig;
import io.debezium.sink.filter.FieldFilterFactory;
import io.debezium.sink.filter.FieldFilterFactory.FieldNameFilter;
import io.debezium.sink.naming.CollectionNamingStrategy;
import io.debezium.sink.naming.ColumnNamingStrategy;
import io.debezium.sink.naming.DefaultColumnNamingStrategy;
import io.debezium.util.Strings;

public class MongoDbSinkConnectorConfig implements SharedMongoDbConnectorConfig, SinkConnectorConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbSinkConnectorConfig.class);

    public static final String ID_FIELD = "_id";

    public static final String SINK_DATABASE = "sink.database";
    public static final String COLUMN_NAMING_STRATEGY = "column.naming.strategy";
    public static final String FIELD_INCLUDE_LIST = "field.include.list";
    public static final String FIELD_EXCLUDE_LIST = "field.exclude.list";

    public static final Field SINK_DATABASE_NAME = Field.create(SINK_DATABASE)
            .withDisplayName("The sink MongoDB database name.")
            .withType(ConfigDef.Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 2))
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("The name of the MongoDB database to which the connector writes to.")
            .required();

    public static final Field COLUMN_NAMING_STRATEGY_FIELD = Field.create(COLUMN_NAMING_STRATEGY)
            .withDisplayName("ColumnNamingStrategy class")
            .withType(ConfigDef.Type.CLASS)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 3))
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(DefaultColumnNamingStrategy.class.getName())
            .withDescription("The fully qualified name of the class that provide the column naming strategy. It must implement the ColumnNamingStrategy interface.");

    protected static final ConfigDefinition CONFIG_DEFINITION = ConfigDefinition.editor()
            .connector(
                    SINK_DATABASE_NAME,
                    CONNECTION_STRING,
                    COLLECTION_NAMING_STRATEGY_FIELD,
                    COLLECTION_NAME_FORMAT_FIELD,
                    COLUMN_NAMING_STRATEGY_FIELD,
                    BATCH_SIZE_FIELD)
            .create();

    /**
     * The set of {@link Field}s defined as part of this configuration.
     */
    public static Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    public WriteModelStrategy getWriteModelStrategy() {
        return new DefaultWriteModelStrategy();
    }

    public WriteModelStrategy getDeleteWriteModelStrategy() {
        return new DeleteDefaultStrategy();
    }

    private final Configuration config;

    private final ConnectionString connectionString;

    private final String sinkDatabaseName;
    private final String collectionNameFormat;
    private final CollectionNamingStrategy collectionNamingStrategy;
    private final ColumnNamingStrategy columnNamingStrategy;
    private FieldFilterFactory.FieldNameFilter fieldsFilter;
    private final int batchSize;
    private final boolean truncateEnabled;
    private final boolean deleteEnabled;
    private final String cloudEventsSchemaNamePattern;

    public MongoDbSinkConnectorConfig(Configuration config) {
        this.config = config;
        this.connectionString = resolveConnectionString(config);
        this.sinkDatabaseName = config.getString(SINK_DATABASE_NAME);

        this.collectionNameFormat = config.getString(COLLECTION_NAME_FORMAT_FIELD);
        this.collectionNamingStrategy = config.getInstance(COLLECTION_NAMING_STRATEGY_FIELD, CollectionNamingStrategy.class);
        this.columnNamingStrategy = config.getInstance(COLUMN_NAMING_STRATEGY_FIELD, ColumnNamingStrategy.class);

        String fieldExcludeList = config.getString(FIELD_EXCLUDE_LIST);
        String fieldIncludeList = config.getString(FIELD_INCLUDE_LIST);
        this.fieldsFilter = FieldFilterFactory.createFieldFilter(fieldIncludeList, fieldExcludeList);
        this.truncateEnabled = config.getBoolean(SinkConnectorConfig.TRUNCATE_ENABLED_FIELD);
        this.deleteEnabled = config.getBoolean(DELETE_ENABLED_FIELD);
        this.batchSize = config.getInteger(BATCH_SIZE_FIELD);
        this.cloudEventsSchemaNamePattern = config.getString(CLOUDEVENTS_SCHEMA_NAME_PATTERN_FIELD);
    }

    public void validate() {
        if (!config.validateAndRecord(MongoDbSinkConnectorConfig.ALL_FIELDS, LOGGER::error)) {
            throw new ConnectException("Error configuring an instance of " + getClass().getSimpleName() + "; check the logs for details");
        }

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Starting {} with configuration:", getClass().getSimpleName());
            config.withMaskedPasswords().forEach((propName, propValue) -> {
                LOGGER.info("   {} = {}", propName, propValue);
            });
        }

        String columnExcludeList = config.getString(FIELD_EXCLUDE_LIST);
        String columnIncludeList = config.getString(FIELD_INCLUDE_LIST);

        if (!Strings.isNullOrEmpty(columnExcludeList) && !Strings.isNullOrEmpty(columnIncludeList)) {
            throw new ConnectException("Cannot define both column.exclude.list and column.include.list. Please specify only one.");
        }
    }

    public boolean validateAndRecord(Iterable<Field> fields, Consumer<String> problems) {
        return config.validateAndRecord(fields, problems);
    }

    public static ConfigDef configDef() {
        return CONFIG_DEFINITION.configDef();
    }

    @Override
    public int getBatchSize() {
        return batchSize;
    }

    @Override
    public CollectionNamingStrategy getCollectionNamingStrategy() {
        return collectionNamingStrategy;
    }

    @Override
    public FieldNameFilter getFieldFilter() {
        return fieldsFilter;
    }

    public ColumnNamingStrategy getColumnNamingStrategy() {
        return columnNamingStrategy;
    }

    public String getContextName() {
        return Module.contextName();
    }

    public String getConnectorName() {
        return Module.name();
    }

    public ConnectionString getConnectionString() {
        return this.connectionString;
    }

    public String getSinkDatabaseName() {
        return sinkDatabaseName;
    }

    @Override
    public String getCollectionNameFormat() {
        return collectionNameFormat;
    }

    @Override
    public PrimaryKeyMode getPrimaryKeyMode() {
        return null;
    }

    @Override
    public boolean isTruncateEnabled() {
        return truncateEnabled;
    }

    @Override
    public boolean isDeleteEnabled() {
        return deleteEnabled;
    }

    @Override
    public String useTimeZone() {
        return "UTC";
    }

    @Override
    public String cloudEventsSchemaNamePattern() {
        return cloudEventsSchemaNamePattern;
    }

}
