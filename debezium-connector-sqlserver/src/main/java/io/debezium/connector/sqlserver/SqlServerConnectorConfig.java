/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.document.Document;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.util.Strings;

/**
 * The list of configuration options for SQL Server connector
 *
 * @author Jiri Pechanec
 */
public class SqlServerConnectorConfig extends HistorizedRelationalDatabaseConnectorConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerConnectorConfig.class);

    public static final String MAX_TRANSACTIONS_PER_ITERATION_CONFIG_NAME = "max.iteration.transactions";
    public static final String ERRORS_MAX_RETRIES = "errors.max.retries";
    protected static final int DEFAULT_PORT = 1433;
    protected static final int DEFAULT_MAX_TRANSACTIONS_PER_ITERATION = 0;
    protected static final int DEFAULT_MAX_RETRIES = -1;
    private static final String READ_ONLY_INTENT = "ReadOnly";
    private static final String APPLICATION_INTENT_KEY = "database.applicationIntent";

    /**
     * The set of predefined SnapshotMode options or aliases.
     */
    public enum SnapshotMode implements EnumeratedValue {

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

        SnapshotMode(String value, boolean includeData) {
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
    public enum SnapshotIsolationMode implements EnumeratedValue {

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

        SnapshotIsolationMode(String value) {
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

    public static final Field USER = RelationalDatabaseConnectorConfig.USER
            .optional()
            .withNoValidation();

    public static final Field PORT = RelationalDatabaseConnectorConfig.PORT
            .withDefault(DEFAULT_PORT);

    public static final Field INSTANCE = Field.create(DATABASE_CONFIG_PREFIX + SqlServerConnection.INSTANCE_NAME)
            .withDisplayName("Instance name")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 8))
            .withImportance(Importance.LOW)
            .withValidation(Field::isOptional)
            .withDescription("The SQL Server instance name");

    public static final Field DATABASE_NAMES = Field.create(DATABASE_CONFIG_PREFIX + "names")
            .withDisplayName("Databases")
            .withType(Type.LIST)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 7))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withValidation(SqlServerConnectorConfig::validateDatabaseNames)
            .withDescription("The names of the databases from which the connector should capture changes");

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
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 1))
            .withImportance(Importance.MEDIUM)
            .withValidation(Field::isNonNegativeInteger)
            .withDescription("This property can be used to reduce the connector memory usage footprint when changes are streamed from multiple tables per database.");

    public static final Field MAX_RETRIES_ON_ERROR = Field.create(ERRORS_MAX_RETRIES)
            .withDisplayName("The maximum number of retries")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 2))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDefault(DEFAULT_MAX_RETRIES)
            .withValidation(Field::isInteger)
            .withDescription(
                    "The maximum number of retries on connection errors before failing (-1 = no limit, 0 = disabled, > 0 = num of retries).");

    public static final Field SNAPSHOT_MODE = Field.create("snapshot.mode")
            .withDisplayName("Snapshot mode")
            .withEnum(SnapshotMode.class, SnapshotMode.INITIAL)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 0))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("The criteria for running a snapshot upon startup of the connector. "
                    + "Options include: "
                    + "'initial' (the default) to specify the connector should run a snapshot only when no offsets are available for the logical server name; "
                    + "'schema_only' to specify the connector should run a snapshot of the schema when no offsets are available for the logical server name. ");

    public static final Field SNAPSHOT_ISOLATION_MODE = Field.create("snapshot.isolation.mode")
            .withDisplayName("Snapshot isolation mode")
            .withEnum(SnapshotIsolationMode.class, SnapshotIsolationMode.REPEATABLE_READ)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 1))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Controls which transaction isolation level is used and how long the connector locks the captured tables. "
                    + "The default is '" + SnapshotIsolationMode.REPEATABLE_READ.getValue()
                    + "', which means that repeatable read isolation level is used. In addition, exclusive locks are taken only during schema snapshot. "
                    + "Using a value of '" + SnapshotIsolationMode.EXCLUSIVE.getValue()
                    + "' ensures that the connector holds the exclusive lock (and thus prevents any reads and updates) for all captured tables during the entire snapshot duration. "
                    + "When '" + SnapshotIsolationMode.SNAPSHOT.getValue()
                    + "' is specified, connector runs the initial snapshot in SNAPSHOT isolation level, which guarantees snapshot consistency. In addition, neither table nor row-level locks are held. "
                    + "When '" + SnapshotIsolationMode.READ_COMMITTED.getValue()
                    + "' is specified, connector runs the initial snapshot in READ COMMITTED isolation level. No long-running locks are taken, so that initial snapshot does not prevent "
                    + "other transactions from updating table rows. Snapshot consistency is not guaranteed."
                    + "In '" + SnapshotIsolationMode.READ_UNCOMMITTED.getValue()
                    + "' mode neither table nor row-level locks are acquired, but connector does not guarantee snapshot consistency.");

    public static final Field INCREMENTAL_SNAPSHOT_OPTION_RECOMPILE = Field.create("incremental.snapshot.option.recompile")
            .withDisplayName("Recompile SELECT statements")
            .withDefault(false)
            .withType(Type.BOOLEAN)
            .withImportance(Importance.LOW)
            .withValidation(Field::isBoolean)
            .withDescription("Add OPTION(RECOMPILE) on each SELECT statement during the incremental snapshot process. "
                    + "This prevents parameter sniffing but can cause CPU pressure on the source database.");

    private static final ConfigDefinition CONFIG_DEFINITION = HistorizedRelationalDatabaseConnectorConfig.CONFIG_DEFINITION.edit()
            .name("SQL Server")
            .type(
                    DATABASE_NAMES,
                    HOSTNAME,
                    PORT,
                    USER,
                    PASSWORD,
                    INSTANCE)
            .connector(
                    SNAPSHOT_MODE,
                    SNAPSHOT_ISOLATION_MODE,
                    MAX_TRANSACTIONS_PER_ITERATION,
                    BINARY_HANDLING_MODE,
                    SCHEMA_NAME_ADJUSTMENT_MODE,
                    INCREMENTAL_SNAPSHOT_OPTION_RECOMPILE,
                    INCREMENTAL_SNAPSHOT_CHUNK_SIZE,
                    INCREMENTAL_SNAPSHOT_ALLOW_SCHEMA_CHANGES,
                    MAX_RETRIES_ON_ERROR)
            .excluding(
                    SCHEMA_INCLUDE_LIST,
                    SCHEMA_EXCLUDE_LIST)
            .create();

    /**
     * The set of {@link Field}s defined as part of this configuration.
     */
    public static Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    public static ConfigDef configDef() {
        return CONFIG_DEFINITION.configDef();
    }

    private final List<String> databaseNames;
    private final String instanceName;
    private final SnapshotMode snapshotMode;
    private final SnapshotIsolationMode snapshotIsolationMode;
    private final boolean readOnlyDatabaseConnection;
    private final int maxTransactionsPerIteration;
    private final int maxRetriesOnError;
    private final boolean optionRecompile;

    public SqlServerConnectorConfig(Configuration config) {
        super(
                SqlServerConnector.class,
                config,
                new SystemTablesPredicate(),
                x -> x.schema() + "." + x.table(),
                true,
                ColumnFilterMode.SCHEMA,
                true);

        final String databaseNames = config.getString(DATABASE_NAMES.name());

        if (databaseNames != null) {
            this.databaseNames = Arrays.asList(databaseNames.split(","));
        }
        else {
            this.databaseNames = Collections.emptyList();
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

        this.maxTransactionsPerIteration = config.getInteger(MAX_TRANSACTIONS_PER_ITERATION);
        this.maxRetriesOnError = config.getInteger(MAX_RETRIES_ON_ERROR);

        if (!config.getBoolean(MAX_LSN_OPTIMIZATION)) {
            LOGGER.warn("The option '{}' is no longer taken into account. The optimization is always enabled.", MAX_LSN_OPTIMIZATION.name());
        }

        this.optionRecompile = config.getBoolean(INCREMENTAL_SNAPSHOT_OPTION_RECOMPILE);
    }

    public List<String> getDatabaseNames() {
        return databaseNames;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public boolean useSingleDatabase() {
        return this.databaseNames.size() == 1;
    }

    @Override
    public SqlServerJdbcConfiguration getJdbcConfig() {
        JdbcConfiguration config = super.getJdbcConfig();
        if (useSingleDatabase()) {
            config = JdbcConfiguration.copy(config)
                    .withDatabase(databaseNames.get(0))
                    .build();
        }
        SqlServerJdbcConfiguration sqlServerconfig = SqlServerJdbcConfiguration.adapt(config);
        if (getInstanceName() != null) {
            sqlServerconfig = SqlServerJdbcConfiguration.copy(config)
                    .withInstance(getInstanceName())
                    .build();
        }
        return sqlServerconfig;
    }

    public SnapshotIsolationMode getSnapshotIsolationMode() {
        return this.snapshotIsolationMode;
    }

    public SnapshotMode getSnapshotMode() {
        return snapshotMode;
    }

    public boolean isReadOnlyDatabaseConnection() {
        return readOnlyDatabaseConnection;
    }

    public int getMaxTransactionsPerIteration() {
        return maxTransactionsPerIteration;
    }

    public int getMaxRetriesOnError() {
        return maxRetriesOnError;
    }

    public boolean getOptionRecompile() {
        return optionRecompile;
    }

    @Override
    public boolean supportsOperationFiltering() {
        return true;
    }

    @Override
    protected boolean supportsSchemaChangesDuringIncrementalSnapshot() {
        return true;
    }

    @Override
    protected SourceInfoStructMaker<? extends AbstractSourceInfo> getSourceInfoStructMaker(Version version) {
        return new SqlServerSourceInfoStructMaker(Module.name(), Module.version(), this);
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

    @Override
    public Map<TableId, String> getSnapshotSelectOverridesByTable() {
        List<String> tableValues = getConfig().getTrimmedStrings(SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE, ",");

        if (tableValues == null) {
            return Collections.emptyMap();
        }

        Map<TableId, String> snapshotSelectOverridesByTable = new HashMap<>();

        for (String table : tableValues) {
            snapshotSelectOverridesByTable.put(
                    TableId.parse(table, new SqlServerTableIdPredicates()),
                    getConfig().getString(SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE + "." + table));
        }

        return Collections.unmodifiableMap(snapshotSelectOverridesByTable);
    }

    private static int validateDatabaseNames(Configuration config, Field field, Field.ValidationOutput problems) {
        String databaseNames = config.getString(field);
        int count = 0;
        if (Strings.isNullOrBlank(databaseNames)) {
            problems.accept(field, databaseNames, "Cannot be empty");
            ++count;
        }

        return count;
    }
}
