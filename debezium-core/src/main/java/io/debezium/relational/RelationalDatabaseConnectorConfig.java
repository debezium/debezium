/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.config.Field.ValidationOutput;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcValueConverters.DecimalMode;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Key.CustomKeyMapper;
import io.debezium.relational.Key.KeyMapper;
import io.debezium.relational.Selectors.TableIdToStringMapper;
import io.debezium.relational.Tables.ColumnNameFilter;
import io.debezium.relational.Tables.ColumnNameFilterFactory;
import io.debezium.relational.Tables.TableFilter;

/**
 * Configuration options shared across the relational CDC connectors.
 *
 * @author Gunnar Morling
 */
public abstract class RelationalDatabaseConnectorConfig extends CommonConnectorConfig {

    protected static final String SCHEMA_INCLUDE_LIST_NAME = "schema.include.list";
    protected static final String SCHEMA_EXCLUDE_LIST_NAME = "schema.exclude.list";
    protected static final String DATABASE_WHITELIST_NAME = "database.whitelist";
    protected static final String DATABASE_INCLUDE_LIST_NAME = "database.include.list";
    protected static final String DATABASE_BLACKLIST_NAME = "database.blacklist";
    protected static final String DATABASE_EXCLUDE_LIST_NAME = "database.exclude.list";
    protected static final String TABLE_BLACKLIST_NAME = "table.blacklist";
    protected static final String TABLE_EXCLUDE_LIST_NAME = "table.exclude.list";
    protected static final String TABLE_WHITELIST_NAME = "table.whitelist";
    protected static final String TABLE_INCLUDE_LIST_NAME = "table.include.list";

    public static final String TABLE_INCLUDE_LIST_ALREADY_SPECIFIED_ERROR_MSG = "\"table.include.list\" is already specified";
    public static final String TABLE_WHITELIST_ALREADY_SPECIFIED_ERROR_MSG = "\"table.whitelist\" is already specified";
    public static final String COLUMN_INCLUDE_LIST_ALREADY_SPECIFIED_ERROR_MSG = "\"column.include.list\" is already specified";
    public static final String COLUMN_WHITELIST_ALREADY_SPECIFIED_ERROR_MSG = "\"column.whitelist\" is already specified";
    public static final String SCHEMA_INCLUDE_LIST_ALREADY_SPECIFIED_ERROR_MSG = "\"schema.include.list\" is already specified";
    public static final String SCHEMA_WHITELIST_ALREADY_SPECIFIED_ERROR_MSG = "\"schema.whitelist\" is already specified";
    public static final String DATABASE_INCLUDE_LIST_ALREADY_SPECIFIED_ERROR_MSG = "\"database.include.list\" is already specified";
    public static final String DATABASE_WHITELIST_ALREADY_SPECIFIED_ERROR_MSG = "\"database.whitelist\" is already specified";

    public static final long DEFAULT_SNAPSHOT_LOCK_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(10);

    /**
     * The set of predefined DecimalHandlingMode options or aliases.
     */
    public enum DecimalHandlingMode implements EnumeratedValue {
        /**
         * Represent {@code DECIMAL} and {@code NUMERIC} values as precise {@link BigDecimal} values, which are
         * represented in change events in a binary form. This is precise but difficult to use.
         */
        PRECISE("precise"),

        /**
         * Represent {@code DECIMAL} and {@code NUMERIC} values as a string values. This is precise, it supports also special values
         * but the type information is lost.
         */
        STRING("string"),

        /**
         * Represent {@code DECIMAL} and {@code NUMERIC} values as precise {@code double} values. This may be less precise
         * but is far easier to use.
         */
        DOUBLE("double");

        private final String value;

        DecimalHandlingMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        public DecimalMode asDecimalMode() {
            switch (this) {
                case DOUBLE:
                    return DecimalMode.DOUBLE;
                case STRING:
                    return DecimalMode.STRING;
                case PRECISE:
                default:
                    return DecimalMode.PRECISE;
            }
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static DecimalHandlingMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (DecimalHandlingMode option : DecimalHandlingMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @param defaultValue the default value; may be null
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static DecimalHandlingMode parse(String value, String defaultValue) {
            DecimalHandlingMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    public static final Field HOSTNAME = Field.create(DATABASE_CONFIG_PREFIX + JdbcConfiguration.HOSTNAME)
            .withDisplayName("Hostname")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withValidation(Field::isRequired)
            .withDescription("Resolvable hostname or IP address of the database server.");

    public static final Field PORT = Field.create(DATABASE_CONFIG_PREFIX + JdbcConfiguration.PORT)
            .withDisplayName("Port")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withImportance(Importance.HIGH)
            .withValidation(Field::isInteger)
            .withDescription("Port of the database server.");

    public static final Field USER = Field.create(DATABASE_CONFIG_PREFIX + JdbcConfiguration.USER)
            .withDisplayName("User")
            .withType(Type.STRING)
            .withWidth(Width.SHORT)
            .withImportance(Importance.HIGH)
            .withValidation(Field::isRequired)
            .withDescription("Name of the database user to be used when connecting to the database.");

    public static final Field PASSWORD = Field.create(DATABASE_CONFIG_PREFIX + JdbcConfiguration.PASSWORD)
            .withDisplayName("Password")
            .withType(Type.PASSWORD)
            .withWidth(Width.SHORT)
            .withImportance(Importance.HIGH)
            .withDescription("Password of the database user to be used when connecting to the database.");

    public static final Field DATABASE_NAME = Field.create(DATABASE_CONFIG_PREFIX + JdbcConfiguration.DATABASE)
            .withDisplayName("Database")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withValidation(Field::isRequired)
            .withDescription("The name of the database the connector should be monitoring");

    public static final Field SERVER_NAME = Field.create(DATABASE_CONFIG_PREFIX + "server.name")
            .withDisplayName("Namespace")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withValidation(Field::isRequired)
            .withDescription("Unique name that identifies the database server and all "
                    + "recorded offsets, and that is used as a prefix for all schemas and topics. "
                    + "Each distinct installation should have a separate namespace and be monitored by "
                    + "at most one Debezium connector.");

    /**
     * A comma-separated list of regular expressions that match the fully-qualified names of tables to be monitored.
     * Fully-qualified names for tables are of the form {@code <databaseName>.<tableName>} or
     * {@code <databaseName>.<schemaName>.<tableName>}. Must not be used with {@link #TABLE_EXCLUDE_LIST}, and superseded by database
     * inclusions/exclusions.
     */
    public static final Field TABLE_INCLUDE_LIST = Field.create(TABLE_INCLUDE_LIST_NAME)
            .withDisplayName("Include Tables")
            .withType(Type.LIST)
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withValidation(Field::isListOfRegex)
            .withDescription("The tables for which changes are to be captured");

    /**
     * Old, backwards-compatible "whitelist" property.
     */
    @Deprecated
    public static final Field TABLE_WHITELIST = Field.create(TABLE_WHITELIST_NAME)
            .withDisplayName("Deprecated: Include Tables")
            .withType(Type.LIST)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withValidation(Field::isListOfRegex)
            .withInvisibleRecommender()
            .withDescription("The tables for which changes are to be captured (deprecated, use \"" + TABLE_INCLUDE_LIST.name() + "\" instead)");

    /**
     * A comma-separated list of regular expressions that match the fully-qualified names of tables to be excluded from
     * monitoring. Fully-qualified names for tables are of the form {@code <databaseName>.<tableName>} or
     * {@code <databaseName>.<schemaName>.<tableName>}. Must not be used with {@link #TABLE_INCLUDE_LIST}.
     */
    public static final Field TABLE_EXCLUDE_LIST = Field.create(TABLE_EXCLUDE_LIST_NAME)
            .withDisplayName("Exclude Tables")
            .withType(Type.LIST)
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withValidation(Field::isListOfRegex, RelationalDatabaseConnectorConfig::validateTableExcludeList)
            .withDescription("A comma-separated list of regular expressions that match the fully-qualified names of tables to be excluded from monitoring");

    /**
     * Old, backwards-compatible "blacklist" property.
     */
    @Deprecated
    public static final Field TABLE_BLACKLIST = Field.create(TABLE_BLACKLIST_NAME)
            .withDisplayName("Deprecated: Exclude Tables")
            .withType(Type.LIST)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withValidation(Field::isListOfRegex, RelationalDatabaseConnectorConfig::validateTableBlacklist)
            .withInvisibleRecommender()
            .withDescription(
                    "A comma-separated list of regular expressions that match the fully-qualified names of tables to be excluded from monitoring (deprecated, use \""
                            + TABLE_EXCLUDE_LIST.name() + "\" instead)");

    public static final Field TABLE_IGNORE_BUILTIN = Field.create("table.ignore.builtin")
            .withDisplayName("Ignore system databases")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(true)
            .withValidation(Field::isBoolean)
            .withDescription("Flag specifying whether built-in tables should be ignored.");

    /**
     * A comma-separated list of regular expressions that match fully-qualified names of columns to be excluded from monitoring
     * and change messages. The exact form of fully qualified names for columns might vary between connector types.
     * For instance, they could be of the form {@code <databaseName>.<tableName>.<columnName>} or
     * {@code <schemaName>.<tableName>.<columnName>} or {@code <databaseName>.<schemaName>.<tableName>.<columnName>}.
     */
    public static final Field COLUMN_EXCLUDE_LIST = Field.create("column.exclude.list")
            .withDisplayName("Exclude Columns")
            .withType(Type.LIST)
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withValidation(Field::isListOfRegex, RelationalDatabaseConnectorConfig::validateColumnExcludeList)
            .withDescription("Regular expressions matching columns to exclude from change events");

    /**
     * Old, backwards-compatible "blacklist" property.
     */
    @Deprecated
    public static final Field COLUMN_BLACKLIST = Field.create("column.blacklist")
            .withDisplayName("Deprecated: Exclude Columns")
            .withType(Type.LIST)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withValidation(Field::isListOfRegex, RelationalDatabaseConnectorConfig::validateColumnBlacklist)
            .withInvisibleRecommender()
            .withDescription("Regular expressions matching columns to exclude from change events (deprecated, use \"" + COLUMN_EXCLUDE_LIST.name() + "\" instead)");

    /**
     * A comma-separated list of regular expressions that match fully-qualified names of columns to be excluded from monitoring
     * and change messages. The exact form of fully qualified names for columns might vary between connector types.
     * For instance, they could be of the form {@code <databaseName>.<tableName>.<columnName>} or
     * {@code <schemaName>.<tableName>.<columnName>} or {@code <databaseName>.<schemaName>.<tableName>.<columnName>}.
     */
    public static final Field COLUMN_INCLUDE_LIST = Field.create("column.include.list")
            .withDisplayName("Include Columns")
            .withType(Type.LIST)
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withValidation(Field::isListOfRegex)
            .withDescription("Regular expressions matching columns to include in change events");

    /**
     * Old, backwards-compatible "whitelist" property.
     */
    @Deprecated
    public static final Field COLUMN_WHITELIST = Field.create("column.whitelist")
            .withDisplayName("Deprecated: Include Columns")
            .withType(Type.LIST)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withValidation(Field::isListOfRegex)
            .withInvisibleRecommender()
            .withDescription("Regular expressions matching columns to include in change events (deprecated, use \"" + COLUMN_INCLUDE_LIST.name() + "\" instead)");

    public static final Field MSG_KEY_COLUMNS = Field.create("message.key.columns")
            .withDisplayName("Columns PK mapping")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withValidation(RelationalDatabaseConnectorConfig::validateMessageKeyColumnsField)
            .withDescription("A semicolon-separated list of expressions that match fully-qualified tables and column(s) to be used as message key. "
                    + "Each expression must match the pattern '<fully-qualified table name>:<key columns>',"
                    + "where the table names could be defined as (DB_NAME.TABLE_NAME) or (SCHEMA_NAME.TABLE_NAME), depending on the specific connector,"
                    + "and the key columns are a comma-separated list of columns representing the custom key. "
                    + "For any table without an explicit key configuration the table's primary key column(s) will be used as message key."
                    + "Example: dbserver1.inventory.orderlines:orderId,orderLineId;dbserver1.inventory.orders:id");

    public static final Field DECIMAL_HANDLING_MODE = Field.create("decimal.handling.mode")
            .withDisplayName("Decimal Handling")
            .withEnum(DecimalHandlingMode.class, DecimalHandlingMode.PRECISE)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Specify how DECIMAL and NUMERIC columns should be represented in change events, including:"
                    + "'precise' (the default) uses java.math.BigDecimal to represent values, which are encoded in the change events using a binary representation and Kafka Connect's 'org.apache.kafka.connect.data.Decimal' type; "
                    + "'string' uses string to represent values; "
                    + "'double' represents values using Java's 'double', which may not offer the precision but will be far easier to use in consumers.");

    public static final Field SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE = Field.create("snapshot.select.statement.overrides")
            .withDisplayName("List of tables where the default select statement used during snapshotting should be overridden.")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDescription(" This property contains a comma-separated list of fully-qualified tables (DB_NAME.TABLE_NAME) or (SCHEMA_NAME.TABLE_NAME), depending on the"
                    +
                    "specific connectors . Select statements for the individual tables are " +
                    "specified in further configuration properties, one for each table, identified by the id 'snapshot.select.statement.overrides.[DB_NAME].[TABLE_NAME]' or "
                    +
                    "'snapshot.select.statement.overrides.[SCHEMA_NAME].[TABLE_NAME]', respectively. " +
                    "The value of those properties is the select statement to use when retrieving data from the specific table during snapshotting. " +
                    "A possible use case for large append-only tables is setting a specific point where to start (resume) snapshotting, in case a previous snapshotting was interrupted.");

    /**
     * A comma-separated list of regular expressions that match schema names to be monitored.
     * Must not be used with {@link #SCHEMA_EXCLUDE_LIST}.
     */
    public static final Field SCHEMA_INCLUDE_LIST = Field.create(SCHEMA_INCLUDE_LIST_NAME)
            .withDisplayName("Include Schemas")
            .withType(Type.LIST)
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withValidation(Field::isListOfRegex)
            .withDependents(TABLE_INCLUDE_LIST_NAME)
            .withDescription("The schemas for which events should be captured");

    /**
     * Old, backwards-compatible "whitelist" property.
     */
    @Deprecated
    public static final Field SCHEMA_WHITELIST = Field.create("schema.whitelist")
            .withDisplayName("Deprecated: Include Schemas")
            .withType(Type.LIST)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withValidation(Field::isListOfRegex)
            .withDependents(TABLE_INCLUDE_LIST_NAME)
            .withInvisibleRecommender()
            .withDescription("The schemas for which events should be captured (deprecated, use \"" + SCHEMA_INCLUDE_LIST.name() + "\" instead)");

    /**
     * A comma-separated list of regular expressions that match schema names to be excluded from monitoring.
     * Must not be used with {@link #SCHEMA_INCLUDE_LIST}.
     */
    public static final Field SCHEMA_EXCLUDE_LIST = Field.create(SCHEMA_EXCLUDE_LIST_NAME)
            .withDisplayName("Exclude Schemas")
            .withType(Type.LIST)
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withValidation(Field::isListOfRegex, RelationalDatabaseConnectorConfig::validateSchemaExcludeList)
            .withInvisibleRecommender()
            .withDescription("The schemas for which events must not be captured");

    /**
     * Old, backwards-compatible "blacklist" property.
     */
    @Deprecated
    public static final Field SCHEMA_BLACKLIST = Field.create("schema.blacklist")
            .withDisplayName("Deprecated: Exclude Schemas")
            .withType(Type.LIST)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withValidation(Field::isListOfRegex, RelationalDatabaseConnectorConfig::validateSchemaBlacklist)
            .withInvisibleRecommender()
            .withDescription("The schemas for which events must not be captured (deprecated, use \"" + SCHEMA_EXCLUDE_LIST.name() + "\" instead)");

    /**
     * A comma-separated list of regular expressions that match database names to be monitored.
     * Must not be used with {@link #DATABASE_BLACKLIST}.
     */
    public static final Field DATABASE_INCLUDE_LIST = Field.create(DATABASE_INCLUDE_LIST_NAME)
            .withDisplayName("Include Databases")
            .withType(Type.LIST)
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withDependents(TABLE_INCLUDE_LIST_NAME, TABLE_WHITELIST_NAME)
            .withValidation(Field::isListOfRegex)
            .withDescription("The databases for which changes are to be captured");

    @Deprecated
    public static final Field DATABASE_WHITELIST = Field.create(DATABASE_WHITELIST_NAME)
            .withDisplayName("Deprecated: Include Databases")
            .withType(Type.LIST)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withValidation(Field::isListOfRegex)
            .withInvisibleRecommender()
            .withDescription("The databases for which changes are to be captured (deprecated, use \"" + DATABASE_INCLUDE_LIST.name() + "\" instead)");

    /**
     * A comma-separated list of regular expressions that match database names to be excluded from monitoring.
     * Must not be used with {@link #DATABASE_INCLUDE_LIST}.
     */
    public static final Field DATABASE_EXCLUDE_LIST = Field.create(DATABASE_EXCLUDE_LIST_NAME)
            .withDisplayName("Exclude Databases")
            .withType(Type.LIST)
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withValidation(Field::isListOfRegex, RelationalDatabaseConnectorConfig::validateDatabaseExcludeList)
            .withDescription("A comma-separated list of regular expressions that match database names to be excluded from monitoring");

    @Deprecated
    public static final Field DATABASE_BLACKLIST = Field.create(DATABASE_BLACKLIST_NAME)
            .withDisplayName("Deprecated: Exclude Databases")
            .withType(Type.LIST)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withValidation(Field::isListOfRegex, RelationalDatabaseConnectorConfig::validateDatabaseBlacklist)
            .withInvisibleRecommender()
            .withDescription("A comma-separated list of regular expressions that match database names to be excluded from monitoring (deprecated, use \""
                    + DATABASE_EXCLUDE_LIST.name() + "\" instead)");

    public static final Field TIME_PRECISION_MODE = Field.create("time.precision.mode")
            .withDisplayName("Time Precision")
            .withEnum(TemporalPrecisionMode.class, TemporalPrecisionMode.ADAPTIVE)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Time, date, and timestamps can be represented with different kinds of precisions, including:"
                    + "'adaptive' (the default) bases the precision of time, date, and timestamp values on the database column's precision; "
                    + "'adaptive_time_microseconds' like 'adaptive' mode, but TIME fields always use microseconds precision;"
                    + "'connect' always represents time, date, and timestamp values using Kafka Connect's built-in representations for Time, Date, and Timestamp, "
                    + "which uses millisecond precision regardless of the database columns' precision .");

    public static final Field SNAPSHOT_LOCK_TIMEOUT_MS = Field.create("snapshot.lock.timeout.ms")
            .withDisplayName("Snapshot lock timeout (ms)")
            .withWidth(Width.LONG)
            .withType(Type.LONG)
            .withImportance(Importance.MEDIUM)
            .withDefault(DEFAULT_SNAPSHOT_LOCK_TIMEOUT_MILLIS)
            .withDescription("The maximum number of millis to wait for table locks at the beginning of a snapshot. If locks cannot be acquired in this " +
                    "time frame, the snapshot will be aborted. Defaults to 10 seconds");

    // TODO - belongs to HistorizedRelationalDatabaseConnectorConfig but should be move there
    // after MySQL rewrite
    public static final Field INCLUDE_SCHEMA_CHANGES = Field.create("include.schema.changes")
            .withDisplayName("Include database schema changes")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Whether the connector should publish changes in the database schema to a Kafka topic with "
                    + "the same name as the database server ID. Each schema change will be recorded using a key that "
                    + "contains the database name and whose value include logical description of the new schema and optionally the DDL statement(s)."
                    + "The default is 'true'. This is independent of how the connector internally records database history.")
            .withDefault(true);

    public static final Field MASK_COLUMN_WITH_HASH = Field.create("column.mask.hash.([^.]+).with.salt.(.+)")
            .withDisplayName("Mask Columns Using Hash and Salt")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDescription("A comma-separated list of regular expressions matching fully-qualified names of columns that should "
                    + "be masked by hashing the input. Using the specified hash algorithms and salt.");

    public static final Field MASK_COLUMN = Field.create("column.mask.with.(d+).chars")
            .withDisplayName("Mask Columns With n Asterisks")
            .withValidation(Field::isInteger)
            .withDescription("A comma-separated list of regular expressions matching fully-qualified names of columns that should "
                    + "be masked with configured amount of asterisk ('*') characters.");

    public static final Field TRUNCATE_COLUMN = Field.create("column.truncate.to.(d+).chars")
            .withDisplayName("Truncate Columns To n Characters")
            .withValidation(Field::isInteger)
            .withDescription("A comma-separated list of regular expressions matching fully-qualified names of columns that should "
                    + "be truncated to the configured amount of characters.");

    public static final Field PROPAGATE_COLUMN_SOURCE_TYPE = Field.create("column.propagate.source.type")
            .withDisplayName("Propagate Source Types by Columns")
            .withType(Type.LIST)
            .withValidation(Field::isListOfRegex)
            .withDescription("A comma-separated list of regular expressions matching fully-qualified names of columns that "
                    + " adds the column’s original type and original length as parameters to the corresponding field schemas in the emitted change records.");

    public static final Field PROPAGATE_DATATYPE_SOURCE_TYPE = Field.create("datatype.propagate.source.type")
            .withDisplayName("Propagate Source Types by Data Type")
            .withType(Type.LIST)
            .withValidation(Field::isListOfRegex)
            .withDescription("A comma-separated list of regular expressions matching the database-specific data type names that "
                    + "adds the data type's original type and original length as parameters to the corresponding field schemas in the emitted change records.");

    public static final Field SNAPSHOT_FULL_COLUMN_SCAN_FORCE = Field.createInternal("snapshot.scan.all.columns.force")
            .withDisplayName("Snapshot force scan all columns of all tables")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Restore pre 1.5 behaviour and scan all tables to discover columns."
                    + " If you are excluding one table then turning this on may improve performance."
                    + " If you are excluding a lot of tables the default behavour should work well.")
            .withDefault(false);

    protected static final ConfigDefinition CONFIG_DEFINITION = CommonConnectorConfig.CONFIG_DEFINITION.edit()
            .type(
                    SERVER_NAME)
            .connector(
                    DECIMAL_HANDLING_MODE,
                    TIME_PRECISION_MODE,
                    SNAPSHOT_LOCK_TIMEOUT_MS)
            .events(
                    COLUMN_WHITELIST,
                    COLUMN_INCLUDE_LIST,
                    COLUMN_BLACKLIST,
                    COLUMN_EXCLUDE_LIST,
                    TABLE_WHITELIST,
                    TABLE_INCLUDE_LIST,
                    TABLE_BLACKLIST,
                    TABLE_EXCLUDE_LIST,
                    TABLE_IGNORE_BUILTIN,
                    SCHEMA_WHITELIST,
                    SCHEMA_INCLUDE_LIST,
                    SCHEMA_BLACKLIST,
                    SCHEMA_EXCLUDE_LIST,
                    MSG_KEY_COLUMNS,
                    SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE,
                    MASK_COLUMN_WITH_HASH,
                    MASK_COLUMN,
                    TRUNCATE_COLUMN,
                    INCLUDE_SCHEMA_CHANGES,
                    PROPAGATE_COLUMN_SOURCE_TYPE,
                    PROPAGATE_DATATYPE_SOURCE_TYPE,
                    SNAPSHOT_FULL_COLUMN_SCAN_FORCE)
            .create();

    private final RelationalTableFilters tableFilters;
    private final ColumnNameFilter columnFilter;
    private final TemporalPrecisionMode temporalPrecisionMode;
    private final KeyMapper keyMapper;
    private final TableIdToStringMapper tableIdMapper;

    protected RelationalDatabaseConnectorConfig(Configuration config, String logicalName, TableFilter systemTablesFilter,
                                                TableIdToStringMapper tableIdMapper, int defaultSnapshotFetchSize,
                                                ColumnFilterMode columnFilterMode) {
        super(config, logicalName, defaultSnapshotFetchSize);

        this.temporalPrecisionMode = TemporalPrecisionMode.parse(config.getString(TIME_PRECISION_MODE));
        this.keyMapper = CustomKeyMapper.getInstance(config.getString(MSG_KEY_COLUMNS), tableIdMapper);
        this.tableIdMapper = tableIdMapper;

        if (systemTablesFilter != null && tableIdMapper != null) {
            this.tableFilters = new RelationalTableFilters(config, systemTablesFilter, tableIdMapper);
        }
        // handled by sub-classes for the time being
        else {
            this.tableFilters = null;
        }

        String columnExcludeList = config.getFallbackStringProperty(COLUMN_EXCLUDE_LIST, COLUMN_BLACKLIST);
        String columnIncludeList = config.getFallbackStringProperty(COLUMN_INCLUDE_LIST, COLUMN_WHITELIST);

        if (columnIncludeList != null) {
            this.columnFilter = ColumnNameFilterFactory.createIncludeListFilter(columnIncludeList, columnFilterMode);
        }
        else {
            this.columnFilter = ColumnNameFilterFactory.createExcludeListFilter(columnExcludeList, columnFilterMode);
        }
    }

    public RelationalTableFilters getTableFilters() {
        return tableFilters;
    }

    /**
     * Returns the Decimal mode Enum for {@code decimal.handling.mode}
     * configuration. This defaults to {@code precise} if nothing is provided.
     */
    public DecimalMode getDecimalMode() {
        return DecimalHandlingMode
                .parse(this.getConfig().getString(DECIMAL_HANDLING_MODE))
                .asDecimalMode();
    }

    /**
     * Returns the temporal precision mode mode Enum for {@code time.precision.mode}
     * configuration. This defaults to {@code adaptive} if nothing is provided.
     */
    public TemporalPrecisionMode getTemporalPrecisionMode() {
        return temporalPrecisionMode;
    }

    public KeyMapper getKeyMapper() {
        return keyMapper;
    }

    public Duration snapshotLockTimeout() {
        return Duration.ofMillis(getConfig().getLong(SNAPSHOT_LOCK_TIMEOUT_MS));
    }

    public String schemaExcludeList() {
        return getConfig().getFallbackStringProperty(SCHEMA_EXCLUDE_LIST, SCHEMA_BLACKLIST);
    }

    public String schemaIncludeList() {
        return getConfig().getFallbackStringProperty(SCHEMA_INCLUDE_LIST, SCHEMA_WHITELIST);
    }

    public String tableExcludeList() {
        return getConfig().getFallbackStringProperty(TABLE_EXCLUDE_LIST, TABLE_BLACKLIST);
    }

    public String tableIncludeList() {
        return getConfig().getFallbackStringProperty(TABLE_INCLUDE_LIST, TABLE_WHITELIST);
    }

    public ColumnNameFilter getColumnFilter() {
        return columnFilter;
    }

    public Boolean isFullColummnScanRequired() {
        return getConfig().getBoolean(SNAPSHOT_FULL_COLUMN_SCAN_FORCE);
    }

    private static int validateColumnBlacklist(Configuration config, Field field, Field.ValidationOutput problems) {
        String blacklist = config.getFallbackStringPropertyWithWarning(COLUMN_INCLUDE_LIST, COLUMN_WHITELIST);
        String whitelist = config.getFallbackStringPropertyWithWarning(COLUMN_EXCLUDE_LIST, COLUMN_BLACKLIST);

        if (whitelist != null && blacklist != null) {
            problems.accept(COLUMN_BLACKLIST, blacklist, COLUMN_WHITELIST_ALREADY_SPECIFIED_ERROR_MSG);
            return 1;
        }
        return 0;
    }

    private static int validateColumnExcludeList(Configuration config, Field field, Field.ValidationOutput problems) {
        String includeList = config.getString(COLUMN_INCLUDE_LIST);
        String excludeList = config.getString(COLUMN_EXCLUDE_LIST);

        if (includeList != null && excludeList != null) {
            problems.accept(COLUMN_EXCLUDE_LIST, excludeList, COLUMN_INCLUDE_LIST_ALREADY_SPECIFIED_ERROR_MSG);
            return 1;
        }
        return 0;
    }

    @Override
    public boolean isSchemaChangesHistoryEnabled() {
        return getConfig().getBoolean(INCLUDE_SCHEMA_CHANGES);
    }

    public TableIdToStringMapper getTableIdMapper() {
        return tableIdMapper;
    }

    private static int validateTableBlacklist(Configuration config, Field field, ValidationOutput problems) {
        String whitelist = config.getFallbackStringPropertyWithWarning(TABLE_INCLUDE_LIST, TABLE_WHITELIST);
        String blacklist = config.getFallbackStringPropertyWithWarning(TABLE_EXCLUDE_LIST, TABLE_BLACKLIST);

        if (whitelist != null && blacklist != null) {
            problems.accept(TABLE_BLACKLIST, blacklist, TABLE_WHITELIST_ALREADY_SPECIFIED_ERROR_MSG);
            return 1;
        }
        return 0;
    }

    private static int validateTableExcludeList(Configuration config, Field field, ValidationOutput problems) {
        String includeList = config.getString(TABLE_WHITELIST);
        String excludeList = config.getString(TABLE_BLACKLIST);

        if (includeList != null && excludeList != null) {
            problems.accept(TABLE_EXCLUDE_LIST, excludeList, TABLE_INCLUDE_LIST_ALREADY_SPECIFIED_ERROR_MSG);
            return 1;
        }
        return 0;
    }

    /**
     * Returns any SELECT overrides, if present.
     */
    public Map<TableId, String> getSnapshotSelectOverridesByTable() {
        String tableList = getConfig().getString(SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE);

        if (tableList == null) {
            return Collections.emptyMap();
        }

        Map<TableId, String> snapshotSelectOverridesByTable = new HashMap<>();

        for (String table : tableList.split(",")) {
            snapshotSelectOverridesByTable.put(
                    TableId.parse(table),
                    getConfig().getString(SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE + "." + table));
        }

        return Collections.unmodifiableMap(snapshotSelectOverridesByTable);
    }

    private static int validateSchemaBlacklist(Configuration config, Field field, Field.ValidationOutput problems) {
        String whitelist = config.getFallbackStringPropertyWithWarning(SCHEMA_INCLUDE_LIST, SCHEMA_WHITELIST);
        String blacklist = config.getFallbackStringPropertyWithWarning(SCHEMA_EXCLUDE_LIST, SCHEMA_BLACKLIST);

        if (whitelist != null && blacklist != null) {
            problems.accept(SCHEMA_BLACKLIST, blacklist, SCHEMA_WHITELIST_ALREADY_SPECIFIED_ERROR_MSG);
            return 1;
        }
        return 0;
    }

    private static int validateSchemaExcludeList(Configuration config, Field field, Field.ValidationOutput problems) {
        String includeList = config.getString(SCHEMA_INCLUDE_LIST);
        String excludeList = config.getString(SCHEMA_EXCLUDE_LIST);

        if (includeList != null && excludeList != null) {
            problems.accept(SCHEMA_EXCLUDE_LIST, excludeList, SCHEMA_INCLUDE_LIST_ALREADY_SPECIFIED_ERROR_MSG);
            return 1;
        }
        return 0;
    }

    private static int validateDatabaseExcludeList(Configuration config, Field field, ValidationOutput problems) {
        String includeList = config.getString(DATABASE_INCLUDE_LIST);
        String excludeList = config.getString(DATABASE_EXCLUDE_LIST);
        if (includeList != null && excludeList != null) {
            problems.accept(DATABASE_EXCLUDE_LIST, excludeList, DATABASE_INCLUDE_LIST_ALREADY_SPECIFIED_ERROR_MSG);
            return 1;
        }
        return 0;
    }

    private static int validateDatabaseBlacklist(Configuration config, Field field, ValidationOutput problems) {
        String whitelist = config.getFallbackStringPropertyWithWarning(DATABASE_INCLUDE_LIST, DATABASE_WHITELIST);
        String blacklist = config.getFallbackStringPropertyWithWarning(DATABASE_EXCLUDE_LIST, DATABASE_BLACKLIST);
        if (whitelist != null && blacklist != null) {
            problems.accept(DATABASE_BLACKLIST, blacklist, DATABASE_WHITELIST_ALREADY_SPECIFIED_ERROR_MSG);
            return 1;
        }
        return 0;
    }

    private static int validateMessageKeyColumnsField(Configuration config, Field field, Field.ValidationOutput problems) {
        String msgKeyColumns = config.getString(MSG_KEY_COLUMNS);
        int problemCount = 0;

        if (msgKeyColumns != null) {
            if (msgKeyColumns.isEmpty()) {
                problems.accept(MSG_KEY_COLUMNS, "", "Must not be empty");
            }

            for (String substring : CustomKeyMapper.PATTERN_SPLIT.split(msgKeyColumns)) {
                if (!CustomKeyMapper.MSG_KEY_COLUMNS_PATTERN.asPredicate().test(substring)) {
                    problems.accept(MSG_KEY_COLUMNS, substring,
                            substring + " has an invalid format (expecting '" + CustomKeyMapper.MSG_KEY_COLUMNS_PATTERN.pattern() + "')");
                    problemCount++;
                }
            }
        }
        return problemCount;
    }
}
