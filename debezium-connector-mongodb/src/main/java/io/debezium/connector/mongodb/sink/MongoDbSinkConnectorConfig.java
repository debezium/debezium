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
import io.debezium.connector.SinkConnectorConfig;
import io.debezium.connector.mongodb.shared.SharedMongoDbConnectorConfig;
import io.debezium.table.ColumnNamingStrategy;
import io.debezium.table.DefaultColumnNamingStrategy;
import io.debezium.table.DefaultTableNamingStrategy;
import io.debezium.table.FieldFilterFactory;
import io.debezium.table.TableNamingStrategy;
import io.debezium.util.Strings;

public class MongoDbSinkConnectorConfig implements SharedMongoDbConnectorConfig, SinkConnectorConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbSinkConnectorConfig.class);

    public static final String ID_FIELD = "_id";

    public static final String SINK_DATABASE = "sink.database";
    public static final String INSERT_MODE = "insert.mode";
    public static final String DELETE_ENABLED = "delete.enabled";
    public static final String TRUNCATE_ENABLED = "truncate.enabled";
    public static final String TABLE_NAME_FORMAT = "table.name.format";
    public static final String TABLE_NAMING_STRATEGY = "table.naming.strategy";
    public static final String COLUMN_NAMING_STRATEGY = "column.naming.strategy";
    public static final String BATCH_SIZE = "batch.size";
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

    public static final Field TABLE_NAME_FORMAT_FIELD = Field.create(TABLE_NAME_FORMAT)
            .withDisplayName("A format string for the table")
            .withType(ConfigDef.Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 3))
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDefault("${topic}")
            .withDescription("A format string for the table, which may contain '${topic}' as a placeholder for the original topic name.");

    public static final Field TABLE_NAMING_STRATEGY_FIELD = Field.create(TABLE_NAMING_STRATEGY)
            .withDisplayName("Name of the strategy class that implements the TablingNamingStrategy interface")
            .withType(ConfigDef.Type.CLASS)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 2))
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(DefaultTableNamingStrategy.class.getName())
            .withDescription("Name of the strategy class that implements the TableNamingStrategy interface.");

    public static final Field COLUMN_NAMING_STRATEGY_FIELD = Field.create(COLUMN_NAMING_STRATEGY)
            .withDisplayName("Name of the strategy class that implements the ColumnNamingStrategy interface")
            .withType(ConfigDef.Type.CLASS)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 3))
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(DefaultColumnNamingStrategy.class.getName())
            .withDescription("Name of the strategy class that implements the ColumnNamingStrategy interface.");

    public static final Field BATCH_SIZE_FIELD = Field.create(BATCH_SIZE)
            .withDisplayName("Specifies how many records to attempt to batch together into the destination table, when possible. " +
                    "You can also configure the connector’s underlying consumer’s max.poll.records using consumer.override.max.poll.records in the connector configuration.")
            .withType(ConfigDef.Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 4))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDefault(500)
            .withDescription("Specifies how many records to attempt to batch together into the destination table, when possible. " +
                    "You can also configure the connector’s underlying consumer’s max.poll.records using consumer.override.max.poll.records in the connector configuration.");

    protected static final ConfigDefinition CONFIG_DEFINITION = ConfigDefinition.editor()
            .connector(
                    SINK_DATABASE_NAME,
                    CONNECTION_STRING,
                    TABLE_NAMING_STRATEGY_FIELD,
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
    private final String tableNameFormat;
    private final TableNamingStrategy tableNamingStrategy;
    private final ColumnNamingStrategy columnNamingStrategy;
    private FieldFilterFactory.FieldNameFilter fieldsFilter;
    private final int batchSize;

    public MongoDbSinkConnectorConfig(Configuration config) {
        this.config = config;
        this.connectionString = resolveConnectionString(config);
        this.sinkDatabaseName = config.getString(SINK_DATABASE_NAME);

        this.tableNameFormat = config.getString(TABLE_NAME_FORMAT_FIELD);
        this.tableNamingStrategy = config.getInstance(TABLE_NAMING_STRATEGY_FIELD, TableNamingStrategy.class);
        this.columnNamingStrategy = config.getInstance(COLUMN_NAMING_STRATEGY_FIELD, ColumnNamingStrategy.class);
        this.batchSize = config.getInteger(BATCH_SIZE_FIELD);

        String fieldExcludeList = config.getString(FIELD_EXCLUDE_LIST);
        String fieldIncludeList = config.getString(FIELD_INCLUDE_LIST);
        this.fieldsFilter = FieldFilterFactory.createFieldFilter(fieldIncludeList, fieldExcludeList);
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

    public int getBatchSize() {
        return batchSize;
    }

    public TableNamingStrategy getTableNamingStrategy() {
        return tableNamingStrategy;
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

    public String getTableNameFormat() {
        return tableNameFormat;
    }

}
