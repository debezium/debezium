/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.errors.ConnectException;
import org.hibernate.c3p0.internal.C3P0ConnectionProvider;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.tool.schema.Action;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.config.Field.ValidationOutput;
import io.debezium.connector.jdbc.naming.ColumnNamingStrategy;
import io.debezium.connector.jdbc.naming.DefaultColumnNamingStrategy;
import io.debezium.connector.jdbc.naming.TemporaryBackwardCompatibleCollectionNamingStrategyProxy;
import io.debezium.sink.SinkConnectorConfig;
import io.debezium.sink.filter.FieldFilterFactory;
import io.debezium.sink.filter.FieldFilterFactory.FieldNameFilter;
import io.debezium.sink.naming.CollectionNamingStrategy;
import io.debezium.util.Strings;

/**
 * Connector configuration for the JDBC sink.
 *
 * @author Hossein Torabi
 */
public class JdbcSinkConnectorConfig implements SinkConnectorConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSinkConnectorTask.class);

    private static final String HIBERNATE_PREFIX = "hibernate.";

    public static final String CONNECTION_PROVIDER = "connection.provider";
    public static final String CONNECTION_URL = "connection.url";
    public static final String CONNECTION_USER = "connection.username";
    public static final String CONNECTION_PASSWORD = "connection.password";
    public static final String CONNECTION_POOL_MIN_SIZE = "connection.pool.min_size";
    public static final String CONNECTION_POOL_MAX_SIZE = "connection.pool.max_size";
    public static final String CONNECTION_POOL_ACQUIRE_INCREMENT = "connection.pool.acquire_increment";
    public static final String CONNECTION_POOL_TIMEOUT = "connection.pool.timeout";
    public static final String INSERT_MODE = "insert.mode";
    public static final String TRUNCATE_ENABLED = "truncate.enabled";
    public static final String PRIMARY_KEY_FIELDS = "primary.key.fields";
    public static final String SCHEMA_EVOLUTION = "schema.evolution";
    public static final String QUOTE_IDENTIFIERS = "quote.identifiers";
    public static final String COLUMN_NAMING_STRATEGY = "column.naming.strategy";
    public static final String POSTGRES_POSTGIS_SCHEMA = "dialect.postgres.postgis.schema";
    public static final String SQLSERVER_IDENTITY_INSERT = "dialect.sqlserver.identity.insert";
    public static final String USE_REDUCTION_BUFFER = "use.reduction.buffer";
    public static final String FLUSH_MAX_RETRIES = "flush.max.retries";
    public static final String FLUSH_RETRY_DELAY_MS = "flush.retry.delay.ms";
    public static final String CONNECTION_RESTART_ON_ERRORS = "connection.restart.on.errors";

    // todo add support for the ValueConverter contract

    public static final Field CONNECTION_PROVIDER_FIELD = Field.create(CONNECTION_PROVIDER)
            .withDisplayName("Connection provider")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 0))
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(C3P0ConnectionProvider.class.getName())
            .withDescription("Fully qualified class name of the connection provider, defaults to " + C3P0ConnectionProvider.class.getName());

    public static final Field CONNECTION_URL_FIELD = Field.create(CONNECTION_URL)
            .withDisplayName("Hostname")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 1))
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .required()
            .withDescription("Valid JDBC URL");

    public static final Field CONNECTION_USER_FIELD = Field.create(CONNECTION_USER)
            .withDisplayName("User")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 2))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .required()
            .withDescription("Name of the database user to be used when connecting to the connection.");

    public static final Field CONNECTION_PASSWORD_FIELD = Field.create(CONNECTION_PASSWORD)
            .withDisplayName("Password")
            .withType(Type.PASSWORD)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 3))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Password of the database user to be used when connecting to the connection.");

    public static final Field CONNECTION_POOL_MIN_SIZE_FIELD = Field.create(CONNECTION_POOL_MIN_SIZE)
            .withDisplayName("Connection pool minimum size")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 4))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(5)
            .withDescription("Minimum number of connection in the connection pool");

    public static final Field CONNECTION_POOL_MAX_SIZE_FIELD = Field.create(CONNECTION_POOL_MAX_SIZE)
            .withDisplayName("Connection pool maximum size")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 5))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(32)
            .withDescription("Maximum number of connection in the connection pool");

    public static final Field CONNECTION_POOL_ACQUIRE_INCREMENT_FIELD = Field.create(CONNECTION_POOL_ACQUIRE_INCREMENT)
            .withDisplayName("Connection pool acquire increment")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 6))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(32)
            .withDescription("Connection pool acquire increment");

    public static final Field CONNECTION_POOL_TIMEOUT_FIELD = Field.create(CONNECTION_POOL_TIMEOUT)
            .withDisplayName("Connection pool timeout")
            .withType(Type.LONG)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 7))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(1800)
            .withDescription("Connection pool timeout");

    public static final Field INSERT_MODE_FIELD = Field.create(INSERT_MODE)
            .withDisplayName("The insertion mode to use")
            .withEnum(InsertMode.class, InsertMode.INSERT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 1))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withValidation(JdbcSinkConnectorConfig::validateInsertMode)
            .withDescription("The insertion mode to use. " +
                    "'insert' - the default mode, uses standard SQL insert statements; " +
                    "'upsert' - uses upsert semantics for the database if its supported and requires setting primary.key.mode and primary.key.fields;" +
                    "'update' - uses update semantics for the database if its supported.");

    public static final Field DELETE_ENABLED_FIELD = SinkConnectorConfig.DELETE_ENABLED_FIELD
            .withValidation(JdbcSinkConnectorConfig::validateDeleteEnabled);

    public static final Field PRIMARY_KEY_FIELDS_FIELD = Field.create(PRIMARY_KEY_FIELDS)
            .withDisplayName("Comma-separated list of primary key field names")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 5))
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("A comma-separated list of primary key field names. " +
                    "This is interpreted differently depending on " + PRIMARY_KEY_MODE + ".");

    public static final Field SCHEMA_EVOLUTION_FIELD = Field.create(SCHEMA_EVOLUTION)
            .withDisplayName("Controls how schema evolution is handled by the sink connector")
            .withEnum(SchemaEvolutionMode.class, SchemaEvolutionMode.NONE)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 7))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Controls how schema evolution is handled by the sink connector");

    public static final Field QUOTE_IDENTIFIERS_FIELD = Field.create(QUOTE_IDENTIFIERS)
            .withDisplayName("Controls whether table, column, or other identifiers are quoted")
            .withType(Type.BOOLEAN)
            .withDefault(false)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 8))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("When enabled, table, column, and other identifiers are quoted based on the database dialect. " +
                    "When disabled, only explicit cases where the dialect requires quoting will be used, such as names starting with an underscore.");

    public static final Field COLUMN_NAMING_STRATEGY_FIELD = Field.create(COLUMN_NAMING_STRATEGY)
            .withDisplayName("Name of the strategy class that implements the ColumnNamingStrategy interface")
            .withType(Type.CLASS)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 2))
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(DefaultColumnNamingStrategy.class.getName())
            .withDescription("Name of the strategy class that implements the ColumnNamingStrategy interface.");

    public static final Field POSTGRES_POSTGIS_SCHEMA_FIELD = Field.create(POSTGRES_POSTGIS_SCHEMA)
            .withDisplayName("Name of the schema where postgis extension is installed")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 3))
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault("public")
            .withDescription("Name of the schema where postgis extension is installed. Default is public");

    public static final Field SQLSERVER_IDENTITY_INSERT_FIELD = Field.create(SQLSERVER_IDENTITY_INSERT)
            .withDisplayName("Allowing to insert explicit value for identity column in table for SQLSERVER.")
            .withType(Type.BOOLEAN)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 4))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(false)
            .withDescription("Allowing to insert explicit value for identity column in table for SQLSERVER.");

    public static final Field FLUSH_MAX_RETRIES_FIELD = Field.create(FLUSH_MAX_RETRIES)
            .withDisplayName("Max retry count")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 5))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(5)
            .withDescription("Max retry count when fail to flush. In case of retriable exceptions, " +
                    "the flush will be retried for the defined max retry count. Default is 5.");

    public static final Field FLUSH_RETRY_DELAY_MS_FIELD = Field.create(FLUSH_RETRY_DELAY_MS)
            .withDisplayName("Delay to retry flush")
            .withType(Type.LONG)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 6))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(1000L)
            .withDescription("Delay to retry when fail to flush");

    public static final Field USE_REDUCTION_BUFFER_FIELD = Field.create(USE_REDUCTION_BUFFER)
            .withDisplayName("Specifies whether to use the reduction buffer.")
            .withType(Type.BOOLEAN)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 2))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDefault(false)
            .withDescription(
                    "A reduction buffer consolidates the execution of SQL statements by primary key to reduce the SQL load on the target database. When set to false (the default), each incoming event is applied as a logical SQL change. When set to true, incoming events that refer to the same row will be reduced to a single logical change based on the most recent row state.");

    public static final Field CONNECTION_RESTART_ON_ERRORS_FIELD = Field.create(CONNECTION_RESTART_ON_ERRORS)
            .withDisplayName("Restart connection on errors")
            .withType(Type.BOOLEAN)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 7))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(false)
            .withDescription("Specifies whether the connector should attempt to restart the connection automatically upon connection-related errors. " +
                    "Defaults to false. " +
                    "In environments where the sink database uses asynchronous replication, enabling this option may risk data loss or inconsistencies " +
                    "during failover if the replica has not fully caught up with the primary.");

    protected static final ConfigDefinition CONFIG_DEFINITION = ConfigDefinition.editor()
            .connector(
                    CONNECTION_URL_FIELD,
                    CONNECTION_USER_FIELD,
                    CONNECTION_PASSWORD_FIELD,
                    CONNECTION_POOL_MIN_SIZE_FIELD,
                    CONNECTION_POOL_MAX_SIZE_FIELD,
                    CONNECTION_POOL_ACQUIRE_INCREMENT_FIELD,
                    CONNECTION_POOL_TIMEOUT_FIELD,
                    INSERT_MODE_FIELD,
                    DELETE_ENABLED_FIELD,
                    TRUNCATE_ENABLED_FIELD,
                    COLLECTION_NAME_FORMAT_FIELD,
                    PRIMARY_KEY_MODE_FIELD,
                    PRIMARY_KEY_FIELDS_FIELD,
                    SCHEMA_EVOLUTION_FIELD,
                    QUOTE_IDENTIFIERS_FIELD,
                    COLLECTION_NAMING_STRATEGY_FIELD,
                    COLUMN_NAMING_STRATEGY_FIELD,
                    USE_TIME_ZONE_FIELD,
                    POSTGRES_POSTGIS_SCHEMA_FIELD,
                    SQLSERVER_IDENTITY_INSERT_FIELD,
                    BATCH_SIZE_FIELD,
                    FIELD_INCLUDE_LIST_FIELD,
                    FIELD_EXCLUDE_LIST_FIELD,
                    FLUSH_MAX_RETRIES_FIELD,
                    FLUSH_RETRY_DELAY_MS_FIELD,
                    CONNECTION_RESTART_ON_ERRORS_FIELD)
            .create();

    /**
     * The set of {@link Field}s defined as part of this configuration.
     */
    public static Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    /**
     * Defines the various different insertion modes supported.
     */
    public enum InsertMode implements EnumeratedValue {
        /**
         * Events that create or change data are treated using standard SQL insert semantics.
         */
        INSERT("insert"),

        /**
         * Events that create or change data are treated using upsert semantics.
         */
        UPSERT("upsert"),

        /**
         * Events that create or change data are treated using standard SQL update semantics.
         */
        UPDATE("update");

        private final String mode;

        InsertMode(String mode) {
            this.mode = mode;
        }

        public static InsertMode parse(String value) {
            for (InsertMode option : InsertMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return InsertMode.INSERT;
        }

        @Override
        public String getValue() {
            return mode;
        }

    }

    /**
     * Different modes that the destination table's schema can be evolved.
     */
    public enum SchemaEvolutionMode implements EnumeratedValue {
        /**
         * No schema evolution occurs, assumed that the destination table's structure matches the event.
         */
        NONE("none"),

        /**
         * When an event is received, the table will be created if it does not exist, and any new fields
         * found in the event will be amended to the existing tables.  Any columns omitted from the event
         * will simply be skipped during inserts and updates.
         */
        BASIC("basic");

        // /**
        // * When an event is received, the table will be created if it does not exist, and any new fields
        // * found in the event will be added to the existing table's schema. Any columns from the table
        // * schema not found in the event will be dropped.
        // */
        // ADVANCED("advanced");
        private final String mode;

        SchemaEvolutionMode(String mode) {
            this.mode = mode;
        }

        public static SchemaEvolutionMode parse(String value) {
            for (SchemaEvolutionMode option : SchemaEvolutionMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return SchemaEvolutionMode.NONE;
        }

        @Override
        public String getValue() {
            return mode;
        }

    }

    private final Configuration config;

    private final InsertMode insertMode;
    private final boolean deleteEnabled;
    private final boolean truncateEnabled;
    private final String collectionNameFormat;
    private final PrimaryKeyMode primaryKeyMode;
    private final Set<String> primaryKeyFields;
    private final SchemaEvolutionMode schemaEvolutionMode;
    private final boolean quoteIdentifiers;
    private final CollectionNamingStrategy collectionNamingStrategy;
    private final ColumnNamingStrategy columnNamingStrategy;
    private final String databaseTimezone;
    private final String postgresPostgisSchema;
    private final boolean sqlServerIdentityInsert;
    private final int flushMaxRetries;
    private final long flushRetryDelayMs;
    private final FieldNameFilter fieldsFilter;
    private final int batchSize;
    private final boolean useReductionBuffer;
    private final boolean connectionRestartOnErrors;

    public JdbcSinkConnectorConfig(Map<String, String> props) {
        config = Configuration.from(props);
        this.insertMode = InsertMode.parse(config.getString(INSERT_MODE));
        this.deleteEnabled = config.getBoolean(DELETE_ENABLED_FIELD);
        this.truncateEnabled = config.getBoolean(TRUNCATE_ENABLED_FIELD);
        this.collectionNameFormat = config.getString(COLLECTION_NAME_FORMAT_FIELD);
        this.primaryKeyMode = PrimaryKeyMode.parse(config.getString(PRIMARY_KEY_MODE_FIELD));
        this.primaryKeyFields = Strings.setOf(config.getString(PRIMARY_KEY_FIELDS_FIELD), String::new);
        this.schemaEvolutionMode = SchemaEvolutionMode.parse(config.getString(SCHEMA_EVOLUTION));
        this.quoteIdentifiers = config.getBoolean(QUOTE_IDENTIFIERS_FIELD);
        this.collectionNamingStrategy = new TemporaryBackwardCompatibleCollectionNamingStrategyProxy(
                config.getInstance(COLLECTION_NAMING_STRATEGY_FIELD, CollectionNamingStrategy.class), this);
        this.columnNamingStrategy = config.getInstance(COLUMN_NAMING_STRATEGY_FIELD, ColumnNamingStrategy.class);
        this.databaseTimezone = config.getString(USE_TIME_ZONE_FIELD);
        this.postgresPostgisSchema = config.getString(POSTGRES_POSTGIS_SCHEMA_FIELD);
        this.sqlServerIdentityInsert = config.getBoolean(SQLSERVER_IDENTITY_INSERT_FIELD);
        this.batchSize = config.getInteger(BATCH_SIZE_FIELD);
        this.useReductionBuffer = config.getBoolean(USE_REDUCTION_BUFFER_FIELD);
        this.flushMaxRetries = config.getInteger(FLUSH_MAX_RETRIES_FIELD);
        this.flushRetryDelayMs = config.getLong(FLUSH_RETRY_DELAY_MS_FIELD);
        this.connectionRestartOnErrors = config.getBoolean(CONNECTION_RESTART_ON_ERRORS_FIELD);

        String fieldIncludeList = config.getString(FIELD_INCLUDE_LIST_FIELD);
        String fieldExcludeList = config.getString(FIELD_EXCLUDE_LIST_FIELD);
        this.fieldsFilter = FieldFilterFactory.createFieldFilter(fieldIncludeList, fieldExcludeList);
    }

    public void validate() {
        if (!config.validateAndRecord(JdbcSinkConnectorConfig.ALL_FIELDS, LOGGER::error)) {
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

    protected static ConfigDef configDef() {
        return CONFIG_DEFINITION.configDef();
    }

    public InsertMode getInsertMode() {
        return insertMode;
    }

    public boolean isDeleteEnabled() {
        return deleteEnabled;
    }

    public boolean isTruncateEnabled() {
        return truncateEnabled;
    }

    public String getCollectionNameFormat() {
        return collectionNameFormat;
    }

    public PrimaryKeyMode getPrimaryKeyMode() {
        return primaryKeyMode;
    }

    public Set<String> getPrimaryKeyFields() {
        return primaryKeyFields;
    }

    public SchemaEvolutionMode getSchemaEvolutionMode() {
        return schemaEvolutionMode;
    }

    public boolean isQuoteIdentifiers() {
        return quoteIdentifiers;
    }

    public boolean isSqlServerIdentityInsert() {
        return sqlServerIdentityInsert;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public boolean isUseReductionBuffer() {
        return useReductionBuffer;
    }

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

    public String useTimeZone() {
        return databaseTimezone;
    }

    public String getPostgresPostgisSchema() {
        return postgresPostgisSchema;
    }

    public int getFlushMaxRetries() {
        return flushMaxRetries;
    }

    public long getFlushRetryDelayMs() {
        return flushRetryDelayMs;
    }

    public boolean isConnectionRestartOnErrors() {
        return connectionRestartOnErrors;
    }

    /** makes {@link org.hibernate.cfg.Configuration} from connector config
     *
     * @return {@link org.hibernate.cfg.Configuration}
     */
    public org.hibernate.cfg.Configuration getHibernateConfiguration() {
        org.hibernate.cfg.Configuration hibernateConfig = new org.hibernate.cfg.Configuration();
        hibernateConfig.setProperty(AvailableSettings.CONNECTION_PROVIDER, config.getString(CONNECTION_PROVIDER_FIELD));
        hibernateConfig.setProperty(AvailableSettings.JAKARTA_JDBC_URL, config.getString(CONNECTION_URL_FIELD));
        hibernateConfig.setProperty(AvailableSettings.JAKARTA_JDBC_USER, config.getString(CONNECTION_USER_FIELD));
        String password = config.getString(CONNECTION_PASSWORD_FIELD);
        if (password != null && !password.isEmpty()) {
            hibernateConfig.setProperty(AvailableSettings.JAKARTA_JDBC_PASSWORD, password);
        }
        hibernateConfig.setProperty(AvailableSettings.C3P0_MIN_SIZE, config.getString(CONNECTION_POOL_MIN_SIZE_FIELD));
        hibernateConfig.setProperty(AvailableSettings.C3P0_MAX_SIZE, config.getString(CONNECTION_POOL_MAX_SIZE_FIELD));
        hibernateConfig.setProperty(AvailableSettings.C3P0_ACQUIRE_INCREMENT, config.getString(CONNECTION_POOL_ACQUIRE_INCREMENT_FIELD));
        hibernateConfig.setProperty(AvailableSettings.GLOBALLY_QUOTED_IDENTIFIERS, Boolean.toString(config.getBoolean(QUOTE_IDENTIFIERS_FIELD)));
        hibernateConfig.setProperty(AvailableSettings.JDBC_TIME_ZONE, useTimeZone());

        if (LOGGER.isDebugEnabled()) {
            hibernateConfig.setProperty(AvailableSettings.SHOW_SQL, Boolean.toString(true));
        }

        // Allows users to pass additional configuration options to the sink connector using the "hibernate.*" namespace
        config.subset(HIBERNATE_PREFIX, false).forEach(hibernateConfig::setProperty);

        // Explicitly setting these after user provided values to avoid user overriding these
        hibernateConfig.setProperty(AvailableSettings.HBM2DDL_AUTO, Action.NONE.getExternalJpaName());

        return hibernateConfig;
    }

    public String getConnectorName() {
        return Module.name();
    }

    private static int validateInsertMode(Configuration config, Field field, ValidationOutput problems) {
        final InsertMode insertMode = InsertMode.parse(config.getString(field));
        if (InsertMode.UPSERT.equals(insertMode)) {
            if (!config.hasKey(PRIMARY_KEY_MODE) && !config.hasKey(PRIMARY_KEY_FIELDS)) {
                LOGGER.error("When using UPSERT, please define '{}' and '{}'.", PRIMARY_KEY_MODE, PRIMARY_KEY_FIELDS);
                return 1;
            }
            else if (!config.hasKey(PRIMARY_KEY_MODE)) {
                LOGGER.error("When using UPSERT, please define '{}'.", PRIMARY_KEY_MODE);
                return 1;
            }
        }
        return 0;
    }

    private static int validateDeleteEnabled(Configuration config, Field field, ValidationOutput problems) {
        if (config.getBoolean(field)) {
            final PrimaryKeyMode primaryKeyMode = PrimaryKeyMode.parse(config.getString(PRIMARY_KEY_MODE));
            if (!PrimaryKeyMode.RECORD_KEY.equals(primaryKeyMode)) {
                LOGGER.error("When '{}' is set to 'true', the '{}' option must be set to '{}'.",
                        DELETE_ENABLED, PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_KEY.getValue());
                return 1;
            }
        }
        return 0;
    }
}
