/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.time.DateTimeException;
import java.time.ZoneId;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.document.Document;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.history.HistoryRecordComparator;

/**
 * The list of configuration options for SQL Server connector
 *
 * @author Jiri Pechanec
 */
public class SqlServerConnectorConfig extends HistorizedRelationalDatabaseConnectorConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerConnectorConfig.class);

    public static final String SOURCE_TIMESTAMP_MODE_CONFIG_NAME = "source.timestamp.mode";
    public static final String MAX_TRANSACTIONS_PER_ITERATION_CONFIG_NAME = "max.iteration.transactions";
    protected static final int DEFAULT_PORT = 1433;
    protected static final int DEFAULT_MAX_TRANSACTIONS_PER_ITERATION = 0;
    private static final String READ_ONLY_INTENT = "ReadOnly";
    private static final String APPLICATION_INTENT_KEY = "database.applicationIntent";

    /**
     * The set of predefined SnapshotMode options or aliases.
     */
    public static enum SnapshotMode implements EnumeratedValue {

        /**
         * Perform a snapshot of data and schema upon initial startup of a connector.
         */
        INITIAL("initial", true),

        /**
         * Perform a snapshot of data and schema upon initial startup of a connector but does not transition to streaming.
         */
        INITIAL_ONLY("initial_only", true),

        /**
         * Perform a snapshot of the schema but no data upon initial startup of a connector.
         */
        SCHEMA_ONLY("schema_only", false);

        private final String value;
        private final boolean includeData;

        private SnapshotMode(String value, boolean includeData) {
            this.value = value;
            this.includeData = includeData;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Whether this snapshotting mode should include the actual data or just the
         * schema of captured tables.
         */
        public boolean includeData() {
            return includeData;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static SnapshotMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();

            for (SnapshotMode option : SnapshotMode.values()) {
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
        public static SnapshotMode parse(String value, String defaultValue) {
            SnapshotMode mode = parse(value);

            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }

            return mode;
        }
    }

    /**
     * The set of predefined snapshot isolation mode options.
     */
    public static enum SnapshotIsolationMode implements EnumeratedValue {

        /**
         * This mode will block all reads and writes for the entire duration of the snapshot.
         *
         * The connector will execute {@code SELECT * FROM .. WITH (TABLOCKX)}
         */
        EXCLUSIVE("exclusive"),

        /**
         * This mode uses SNAPSHOT isolation level. This way reads and writes are not blocked for the entire duration
         * of the snapshot.  Snapshot consistency is guaranteed as long as DDL statements are not executed at the time.
         */
        SNAPSHOT("snapshot"),

        /**
         * This mode uses REPEATABLE READ isolation level. This mode will avoid taking any table
         * locks during the snapshot process, except schema snapshot phase where exclusive table
         * locks are acquired for a short period.  Since phantom reads can occur, it does not fully
         * guarantee consistency.
         */
        REPEATABLE_READ("repeatable_read"),

        /**
         * This mode uses READ COMMITTED isolation level. This mode does not take any table locks during
         * the snapshot process. In addition, it does not take any long-lasting row-level locks, like
         * in repeatable read isolation level. Snapshot consistency is not guaranteed.
         */
        READ_COMMITTED("read_committed"),

        /**
         * This mode uses READ UNCOMMITTED isolation level. This mode takes neither table locks nor row-level locks
         * during the snapshot process.  This way other transactions are not affected by initial snapshot process.
         * However, snapshot consistency is not guaranteed.
         */
        READ_UNCOMMITTED("read_uncommitted");

        private final String value;

        private SnapshotIsolationMode(String value) {
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
        public static SnapshotIsolationMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (SnapshotIsolationMode option : SnapshotIsolationMode.values()) {
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
        public static SnapshotIsolationMode parse(String value, String defaultValue) {
            SnapshotIsolationMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    public static final Field PORT = RelationalDatabaseConnectorConfig.PORT
            .withDefault(DEFAULT_PORT);

    public static final Field SERVER_NAME = RelationalDatabaseConnectorConfig.SERVER_NAME
            .withValidation(CommonConnectorConfig::validateServerNameIsDifferentFromHistoryTopicName);

    public static final Field INSTANCE = Field.create(DATABASE_CONFIG_PREFIX + SqlServerConnection.INSTANCE_NAME)
            .withDisplayName("Instance name")
            .withType(Type.STRING)
            .withImportance(Importance.LOW)
            .withValidation(Field::isOptional)
            .withDescription("The SQL Server instance name");

    public static final Field DATABASE_NAME = RelationalDatabaseConnectorConfig.DATABASE_NAME
            .withNoValidation()
            .withValidation(SqlServerConnectorConfig::validateDatabaseName);

    public static final Field DATABASE_NAMES = Field.create(DATABASE_CONFIG_PREFIX + "names")
            .withDisplayName("Databases")
            .withType(Type.LIST)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withValidation(SqlServerConnectorConfig::validateDatabaseNames)
            .withDescription("The names of the databases from which the connector should capture changes");

    /**
     * @deprecated The connector will determine the database server timezone offset automatically.
     */
    @Deprecated
    public static final Field SERVER_TIMEZONE = Field.create(DATABASE_CONFIG_PREFIX + SqlServerConnection.SERVER_TIMEZONE_PROP_NAME)
            .withDisplayName("Server timezone")
            .withType(Type.STRING)
            .withImportance(Importance.LOW)
            .withValidation((config, field, problems) -> {
                String value = config.getString(field);
                if (value != null) {
                    try {
                        ZoneId.of(value, ZoneId.SHORT_IDS);
                    }
                    catch (DateTimeException e) {
                        problems.accept(field, value, "The value must be a valid ZoneId");
                        return 1;
                    }
                }
                return 0;
            })
            .withDescription("The timezone of the server used to correctly shift the commit transaction timestamp on the client side"
                    + "Options include: Any valid Java ZoneId (deprecated, the connector will determine the database server timezone offset automatically)");

    public static final Field MAX_LSN_OPTIMIZATION = Field.createInternal("streaming.lsn.optimization")
            .withDisplayName("Max LSN Optimization")
            .withDefault(true)
            .withType(Type.BOOLEAN)
            .withImportance(Importance.LOW)
            .withDescription("This property can be used to enable/disable an optimization that prevents querying the cdc tables on LSNs not correlated to changes.");

    public static final Field MAX_TRANSACTIONS_PER_ITERATION = Field.create(MAX_TRANSACTIONS_PER_ITERATION_CONFIG_NAME)
            .withDisplayName("Max transactions per iteration")
            .withDefault(DEFAULT_MAX_TRANSACTIONS_PER_ITERATION)
            .withType(Type.INT)
            .withImportance(Importance.MEDIUM)
            .withValidation(Field::isNonNegativeInteger)
            .withDescription("This property can be used to reduce the connector memory usage footprint when changes are streamed from multiple tables per database.");

    public static final Field SOURCE_TIMESTAMP_MODE = Field.create(SOURCE_TIMESTAMP_MODE_CONFIG_NAME)
            .withDisplayName("Source timestamp mode")
            .withDefault(SourceTimestampMode.COMMIT.getValue())
            .withType(Type.STRING)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Configures the criteria of the attached timestamp within the source record (ts_ms)." +
                    "Options include:" +
                    "'" + SourceTimestampMode.COMMIT.getValue() + "', (default) the source timestamp is set to the instant where the record was committed in the database"
                    +
                    "'" + SourceTimestampMode.PROCESSING.getValue()
                    + "', (deprecated) the source timestamp is set to the instant where the record was processed by Debezium.");

    public static final Field SNAPSHOT_MODE = Field.create("snapshot.mode")
            .withDisplayName("Snapshot mode")
            .withEnum(SnapshotMode.class, SnapshotMode.INITIAL)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("The criteria for running a snapshot upon startup of the connector. "
                    + "Options include: "
                    + "'initial' (the default) to specify the connector should run a snapshot only when no offsets are available for the logical server name; "
                    + "'schema_only' to specify the connector should run a snapshot of the schema when no offsets are available for the logical server name. ");

    public static final Field SNAPSHOT_ISOLATION_MODE = Field.create("snapshot.isolation.mode")
            .withDisplayName("Snapshot isolation mode")
            .withEnum(SnapshotIsolationMode.class, SnapshotIsolationMode.REPEATABLE_READ)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Controls which transaction isolation level is used and how long the connector locks the monitored tables. "
                    + "The default is '" + SnapshotIsolationMode.REPEATABLE_READ.getValue()
                    + "', which means that repeatable read isolation level is used. In addition, exclusive locks are taken only during schema snapshot. "
                    + "Using a value of '" + SnapshotIsolationMode.EXCLUSIVE.getValue()
                    + "' ensures that the connector holds the exclusive lock (and thus prevents any reads and updates) for all monitored tables during the entire snapshot duration. "
                    + "When '" + SnapshotIsolationMode.SNAPSHOT.getValue()
                    + "' is specified, connector runs the initial snapshot in SNAPSHOT isolation level, which guarantees snapshot consistency. In addition, neither table nor row-level locks are held. "
                    + "When '" + SnapshotIsolationMode.READ_COMMITTED.getValue()
                    + "' is specified, connector runs the initial snapshot in READ COMMITTED isolation level. No long-running locks are taken, so that initial snapshot does not prevent "
                    + "other transactions from updating table rows. Snapshot consistency is not guaranteed."
                    + "In '" + SnapshotIsolationMode.READ_UNCOMMITTED.getValue()
                    + "' mode neither table nor row-level locks are acquired, but connector does not guarantee snapshot consistency.");

    private static final ConfigDefinition CONFIG_DEFINITION = HistorizedRelationalDatabaseConnectorConfig.CONFIG_DEFINITION.edit()
            .name("SQL Server")
            .type(
                    DATABASE_NAME,
                    DATABASE_NAMES,
                    HOSTNAME,
                    PORT,
                    USER,
                    PASSWORD,
                    SERVER_TIMEZONE,
                    INSTANCE)
            .connector(
                    SNAPSHOT_MODE,
                    SNAPSHOT_ISOLATION_MODE,
                    SOURCE_TIMESTAMP_MODE,
                    MAX_TRANSACTIONS_PER_ITERATION,
                    BINARY_HANDLING_MODE)
            .excluding(
                    SCHEMA_WHITELIST,
                    SCHEMA_INCLUDE_LIST,
                    SCHEMA_BLACKLIST,
                    SCHEMA_EXCLUDE_LIST)
            .create();

    /**
     * The set of {@link Field}s defined as part of this configuration.
     */
    public static Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    public static ConfigDef configDef() {
        return CONFIG_DEFINITION.configDef();
    }

    private final String databaseName;
    private final String instanceName;
    private final SnapshotMode snapshotMode;
    private final SnapshotIsolationMode snapshotIsolationMode;
    private final SourceTimestampMode sourceTimestampMode;
    private final boolean readOnlyDatabaseConnection;
    private final int maxTransactionsPerIteration;
    private final boolean multiPartitionMode;

    public SqlServerConnectorConfig(Configuration config) {
        super(SqlServerConnector.class, config, config.getString(SERVER_NAME), new SystemTablesPredicate(), x -> x.schema() + "." + x.table(), true,
                ColumnFilterMode.SCHEMA);

        final String databaseName = config.getString(DATABASE_NAME.name());
        final String databaseNames = config.getString(DATABASE_NAMES.name());

        if (databaseName != null) {
            multiPartitionMode = false;
            this.databaseName = databaseName;
        }
        else if (databaseNames != null) {
            multiPartitionMode = true;
            this.databaseName = databaseNames;
            LOGGER.info("Multi-partition mode is enabled");
        }
        else {
            multiPartitionMode = false;
            this.databaseName = null;
        }

        this.instanceName = config.getString(INSTANCE);
        this.snapshotMode = SnapshotMode.parse(config.getString(SNAPSHOT_MODE), SNAPSHOT_MODE.defaultValueAsString());

        this.readOnlyDatabaseConnection = READ_ONLY_INTENT.equals(config.getString(APPLICATION_INTENT_KEY));
        if (readOnlyDatabaseConnection) {
            this.snapshotIsolationMode = SnapshotIsolationMode.SNAPSHOT;
            LOGGER.info("JDBC connection has set applicationIntent = ReadOnly, switching snapshot isolation mode to {}", SnapshotIsolationMode.SNAPSHOT.name());
        }
        else {
            this.snapshotIsolationMode = SnapshotIsolationMode.parse(config.getString(SNAPSHOT_ISOLATION_MODE), SNAPSHOT_ISOLATION_MODE.defaultValueAsString());
        }

        this.sourceTimestampMode = SourceTimestampMode.fromMode(config.getString(SOURCE_TIMESTAMP_MODE_CONFIG_NAME));
        this.maxTransactionsPerIteration = config.getInteger(MAX_TRANSACTIONS_PER_ITERATION);

        if (!config.getBoolean(MAX_LSN_OPTIMIZATION)) {
            LOGGER.warn("The option '{}' is no longer taken into account. The optimization is always enabled.", MAX_LSN_OPTIMIZATION.name());
        }
    }

    public Configuration jdbcConfig() {
        return getConfig().subset(DATABASE_CONFIG_PREFIX, true);
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public boolean isMultiPartitionModeEnabled() {
        return multiPartitionMode;
    }

    public SnapshotIsolationMode getSnapshotIsolationMode() {
        return this.snapshotIsolationMode;
    }

    public SnapshotMode getSnapshotMode() {
        return snapshotMode;
    }

    public SourceTimestampMode getSourceTimestampMode() {
        return sourceTimestampMode;
    }

    public boolean isReadOnlyDatabaseConnection() {
        return readOnlyDatabaseConnection;
    }

    public int getMaxTransactionsPerIteration() {
        return maxTransactionsPerIteration;
    }

    @Override
    public boolean supportsOperationFiltering() {
        return true;
    }

    @Override
    protected SourceInfoStructMaker<? extends AbstractSourceInfo> getSourceInfoStructMaker(Version version) {
        switch (version) {
            case V1:
                return new LegacyV1SqlServerSourceInfoStructMaker(Module.name(), Module.version(), this);
            default:
                return new SqlServerSourceInfoStructMaker(Module.name(), Module.version(), this);
        }
    }

    private static class SystemTablesPredicate implements TableFilter {

        @Override
        public boolean isIncluded(TableId t) {
            return t.schema() != null && !(t.schema().toLowerCase().equals("cdc") ||
                    t.schema().toLowerCase().equals("sys") ||
                    t.table().toLowerCase().equals("systranschemas"));
        }
    }

    @Override
    protected HistoryRecordComparator getHistoryRecordComparator() {
        return new HistoryRecordComparator() {
            @Override
            protected boolean isPositionAtOrBefore(Document recorded, Document desired) {
                return Lsn.valueOf(recorded.getString(SourceInfo.CHANGE_LSN_KEY))
                        .compareTo(Lsn.valueOf(desired.getString(SourceInfo.CHANGE_LSN_KEY))) < 1;
            }
        };
    }

    @Override
    public String getContextName() {
        return Module.contextName();
    }

    @Override
    public String getConnectorName() {
        return Module.name();
    }

    private static int validateDatabaseName(Configuration config, Field field, Field.ValidationOutput problems) {
        if (!config.hasKey(field) && !config.hasKey(DATABASE_NAMES)) {
            problems.accept(field, null, "Either " + DATABASE_NAME + " or " + DATABASE_NAMES + " must be specified");
            return 1;
        }

        return 0;
    }

    private static int validateDatabaseNames(Configuration config, Field field, Field.ValidationOutput problems) {
        String databaseNames = config.getString(field);
        int count = 0;
        if (databaseNames != null) {
            if (config.hasKey(DATABASE_NAME)) {
                problems.accept(field, null, "Cannot be specified alongside " + DATABASE_NAME);
                ++count;
            }
            if (databaseNames.contains(",")) {
                problems.accept(field, databaseNames, "Only a single database name is currently supported");
                ++count;
            }
        }

        return count;
    }
}
