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
import io.debezium.connector.jdbc.naming.DefaultTableNamingStrategy;
import io.debezium.connector.jdbc.naming.TableNamingStrategy;
import io.debezium.util.Strings;

/**
 * Connector configuration for the JDBC sink.
 *
 * @author Hossein Torabi
 */
public class JdbcSinkConnectorConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSinkConnectorTask.class);

    private static final String HIBERNATE_PREFIX = "hibernate.";
    private static final String DEFAULT_DATABASE_TIME_ZONE = "UTC";

    public static final String CONNECTION_URL = "connection.url";
    public static final String CONNECTION_USER = "connection.username";
    public static final String CONNECTION_PASSWORD = "connection.password";
    public static final String CONNECTION_POOL_MIN_SIZE = "connection.pool.min_size";
    public static final String CONNECTION_POOL_MAX_SIZE = "connection.pool.max_size";
    public static final String CONNECTION_POOL_ACQUIRE_INCREMENT = "connection.pool.acquire_increment";
    public static final String CONNECTION_POOL_TIMEOUT = "connection.pool.timeout";

    public static final String INSERT_MODE = "insert.mode";
    public static final String DELETE_ENABLED = "delete.enabled";
    public static final String TRUNCATE_ENABLED = "truncate.enabled";
    public static final String TABLE_NAME_FORMAT = "table.name.format";
    public static final String PRIMARY_KEY_MODE = "primary.key.mode";
    public static final String PRIMARY_KEY_FIELDS = "primary.key.fields";
    public static final String SCHEMA_EVOLUTION = "schema.evolution";
    public static final String QUOTE_IDENTIFIERS = "quote.identifiers";
    public static final String DATA_TYPE_MAPPING = "data.type.mapping";
    public static final String TABLE_NAMING_STRATEGY = "table.naming.strategy";
    public static final String COLUMN_NAMING_STRATEGY = "column.naming.strategy";
    public static final String DATABASE_TIME_ZONE = "database.time_zone";
    public static final String POSTGRES_POSTGIS_SCHEMA = "dialect.postgres.postgis.schema";
    public static final String SQLSERVER_IDENTITY_INSERT = "dialect.sqlserver.identity.insert";

    // todo add support for the ValueConverter contract

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
            .required()
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

    public static final Field DELETE_ENABLED_FIELD = Field.create(DELETE_ENABLED)
            .withDisplayName("Controls whether records can be deleted by the connector")
            .withType(Type.BOOLEAN)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 2))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(false)
            .withValidation(JdbcSinkConnectorConfig::validateDeleteEnabled)
            .withDescription("Whether to treat `null` record values as deletes. Requires primary.key.mode to be `record.key`.");

    public static final Field TRUNCATE_ENABLED_FIELD = Field.create(TRUNCATE_ENABLED)
            .withDisplayName("Controls whether records can be truncated by the connector")
            .withType(Type.BOOLEAN)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 2))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(false)
            .withValidation(JdbcSinkConnectorConfig::validateDeleteEnabled)
            .withDescription("Whether to process debezium event `t` as truncate statement.");

    public static final Field TABLE_NAME_FORMAT_FIELD = Field.create(TABLE_NAME_FORMAT)
            .withDisplayName("A format string for the table")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 3))
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDefault("${topic}")
            .withDescription("A format string for the table, which may contain '${topic}' as a placeholder for the original topic name.");

    public static final Field PRIMARY_KEY_MODE_FIELD = Field.create(PRIMARY_KEY_MODE)
            .withDisplayName("The primary key mode")
            .withEnum(PrimaryKeyMode.class, PrimaryKeyMode.NONE)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 4))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("The primary key mode");

    public static final Field PRIMARY_KEY_FIELDS_FIELD = Field.create(PRIMARY_KEY_FIELDS)
            .withDisplayName("Comma-separated list of primary key field names")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 5))
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("A comma-separated list of primary key field names. " +
                    "This is interpreted differently depending on " + PRIMARY_KEY_MODE + ".");

    public static final Field DATABASE_TIME_ZONE_FIELD = Field.create(DATABASE_TIME_ZONE)
            .withDisplayName("The timezone used when inserting temporal values.")
            .withDefault(DEFAULT_DATABASE_TIME_ZONE)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 6))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("The timezone used when inserting temporal values. Defaults to UTC.");

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

    // todo: consider repurposing the following to allow user contributed Types

    // // todo: needs tests - is this used?
    // public static final Field DATA_TYPE_MAPPING_FIELD = Field.create(DATA_TYPE_MAPPING)
    // // todo going to assume a format of "<table-name>.<column-name>:<data-type>".
    // // this allows the user to define precisely the data-type of a given table/column tuple.
    // .withDisplayName("todo")
    // .withType(ConfigDef.Type.STRING)
    // .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 1))
    // .withWidth(ConfigDef.Width.LONG)
    // .withImportance(ConfigDef.Importance.LOW)
    // .withDescription("todo");

    public static final Field TABLE_NAMING_STRATEGY_FIELD = Field.create(TABLE_NAMING_STRATEGY)
            .withDisplayName("Name of the strategy class that implements the TablingNamingStrategy interface")
            .withType(Type.CLASS)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 2))
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(DefaultTableNamingStrategy.class.getName())
            .withDescription("Name of the strategy class that implements the TableNamingStrategy interface.");

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
                    TABLE_NAME_FORMAT_FIELD,
                    PRIMARY_KEY_MODE_FIELD,
                    PRIMARY_KEY_FIELDS_FIELD,
                    SCHEMA_EVOLUTION_FIELD,
                    QUOTE_IDENTIFIERS_FIELD,
                    // DATA_TYPE_MAPPING_FIELD,
                    TABLE_NAMING_STRATEGY_FIELD,
                    COLUMN_NAMING_STRATEGY_FIELD,
                    DATABASE_TIME_ZONE_FIELD,
                    POSTGRES_POSTGIS_SCHEMA_FIELD,
                    SQLSERVER_IDENTITY_INSERT_FIELD)
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

        private String mode;

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
     * Different modes that which primary keys are handled.
     */
    public enum PrimaryKeyMode implements EnumeratedValue {
        /**
         * No keys are utilized, meaning tables will be created or managed without any primary keys.
         */
        NONE("none"),

        /**
         * The Kafka event coordinates are used as the primary key, which include the topic name,
         * the partition, and the offset associated with the event.
         */
        KAFKA("kafka"),

        /**
         * Fields from the record key are to be used, which can include struct-based keys or primitives.
         *
         * In the case of primitives, the {@code primary.key.fields} property must be supplied with a
         * single value, which controls what the primary key column name will be in the destination
         * table.
         *
         * In the case of a struct, the {@code primary.key.fields} property is optional and if specified
         * can specify a subset of the record's key fields to for the destination table's primary key.
         */
        RECORD_KEY("record_key"),

        /**
         * Fields from the event's record value are used. The {@code primary.key.fields} property should
         * be specified to identify the fields that will be the basis for the destination table's
         * primary key.
         */
        RECORD_VALUE("record_value"),

        /**
         * Fields from the event's record header are used. The {@code primary.key.fields} property should
         * be specified to identify the fields that will be the basis for the destination table's
         * primary key.
         */
        RECORD_HEADER("record_header");

        private String mode;

        PrimaryKeyMode(String mode) {
            this.mode = mode;
        }

        public static PrimaryKeyMode parse(String value) {
            for (PrimaryKeyMode option : PrimaryKeyMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return PrimaryKeyMode.RECORD_KEY;
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
        private String mode;

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
    private final String tableNameFormat;
    private final PrimaryKeyMode primaryKeyMode;
    private final Set<String> primaryKeyFields;
    private final SchemaEvolutionMode schemaEvolutionMode;
    private final boolean quoteIdentifiers;
    // private final Set<String> dataTypeMapping;
    private final TableNamingStrategy tableNamingStrategy;
    private final ColumnNamingStrategy columnNamingStrategy;
    private final String databaseTimezone;
    private final String postgresPostgisSchema;

    private final boolean sqlServerIdentityInsert;

    public JdbcSinkConnectorConfig(Map<String, String> props) {
        config = Configuration.from(props);
        this.insertMode = InsertMode.parse(config.getString(INSERT_MODE));
        this.deleteEnabled = config.getBoolean(DELETE_ENABLED_FIELD);
        this.truncateEnabled = config.getBoolean(TRUNCATE_ENABLED_FIELD);
        this.tableNameFormat = config.getString(TABLE_NAME_FORMAT_FIELD);
        this.primaryKeyMode = PrimaryKeyMode.parse(config.getString(PRIMARY_KEY_MODE_FIELD));
        this.primaryKeyFields = Strings.setOf(config.getString(PRIMARY_KEY_FIELDS_FIELD), String::new);
        this.schemaEvolutionMode = SchemaEvolutionMode.parse(config.getString(SCHEMA_EVOLUTION));
        this.quoteIdentifiers = config.getBoolean(QUOTE_IDENTIFIERS_FIELD);
        // this.dataTypeMapping = Strings.setOf(config.getString(DATA_TYPE_MAPPING_FIELD), String::new);
        this.tableNamingStrategy = config.getInstance(TABLE_NAMING_STRATEGY_FIELD, TableNamingStrategy.class);
        this.columnNamingStrategy = config.getInstance(COLUMN_NAMING_STRATEGY_FIELD, ColumnNamingStrategy.class);
        this.databaseTimezone = config.getString(DATABASE_TIME_ZONE_FIELD);
        this.postgresPostgisSchema = config.getString(POSTGRES_POSTGIS_SCHEMA_FIELD);
        this.sqlServerIdentityInsert = config.getBoolean(SQLSERVER_IDENTITY_INSERT_FIELD);
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

    public String getTableNameFormat() {
        return tableNameFormat;
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

    // public Set<String> getDataTypeMapping() {
    // return dataTypeMapping;
    // }
    public TableNamingStrategy getTableNamingStrategy() {
        return tableNamingStrategy;
    }

    public ColumnNamingStrategy getColumnNamingStrategy() {
        return columnNamingStrategy;
    }

    public String getDatabaseTimeZone() {
        return databaseTimezone;
    }

    public String getPostgresPostgisSchema() {
        return postgresPostgisSchema;
    }

    /** makes {@link org.hibernate.cfg.Configuration} from connector config
     *
     * @return {@link org.hibernate.cfg.Configuration}
     */
    public org.hibernate.cfg.Configuration getHibernateConfiguration() {
        org.hibernate.cfg.Configuration hibernateConfig = new org.hibernate.cfg.Configuration();
        hibernateConfig.setProperty(AvailableSettings.CONNECTION_PROVIDER, C3P0ConnectionProvider.class.getName());
        hibernateConfig.setProperty(AvailableSettings.URL, config.getString(CONNECTION_URL_FIELD));
        hibernateConfig.setProperty(AvailableSettings.USER, config.getString(CONNECTION_USER_FIELD));
        hibernateConfig.setProperty(AvailableSettings.PASS, config.getString(CONNECTION_PASSWORD_FIELD));
        hibernateConfig.setProperty(AvailableSettings.C3P0_MIN_SIZE, config.getString(CONNECTION_POOL_MIN_SIZE_FIELD));
        hibernateConfig.setProperty(AvailableSettings.C3P0_MAX_SIZE, config.getString(CONNECTION_POOL_MAX_SIZE_FIELD));
        hibernateConfig.setProperty(AvailableSettings.C3P0_ACQUIRE_INCREMENT, config.getString(CONNECTION_POOL_ACQUIRE_INCREMENT_FIELD));
        hibernateConfig.setProperty(AvailableSettings.GLOBALLY_QUOTED_IDENTIFIERS, Boolean.toString(config.getBoolean(QUOTE_IDENTIFIERS_FIELD)));
        hibernateConfig.setProperty(AvailableSettings.JDBC_TIME_ZONE, getDatabaseTimeZone());

        if (LOGGER.isDebugEnabled()) {
            hibernateConfig.setProperty(AvailableSettings.SHOW_SQL, Boolean.toString(true));
        }

        // Allows users to pass additional configuration options to the sink connector using the "hibernate.*" namespace
        config.subset(HIBERNATE_PREFIX, false).forEach((key, value) -> hibernateConfig.setProperty(key, value));

        // Explicitly setting these after user provided values to avoid user overriding these
        hibernateConfig.setProperty(AvailableSettings.HBM2DDL_AUTO, Action.NONE.getExternalJpaName());

        return hibernateConfig;
    }

    public String getContextName() {
        return Module.contextName();
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
            else if (!config.hasKey(PRIMARY_KEY_FIELDS)) {
                final PrimaryKeyMode primaryKeyMode = PrimaryKeyMode.parse(config.getString(PRIMARY_KEY_MODE_FIELD));
                if (PrimaryKeyMode.RECORD_VALUE.equals(primaryKeyMode)) {
                    LOGGER.error("When using UPSERT, please define '{}'.", PRIMARY_KEY_FIELDS);
                    return 1;
                }
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
