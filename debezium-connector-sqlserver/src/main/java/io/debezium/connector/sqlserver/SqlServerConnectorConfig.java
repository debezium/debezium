/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.function.Predicate;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.document.Document;
import io.debezium.function.Predicates;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.ColumnId;
import io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.ColumnNameFilter;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.KafkaDatabaseHistory;

/**
 * The list of configuration options for SQL Server connector
 *
 * @author Jiri Pechanec
 */
public class SqlServerConnectorConfig extends HistorizedRelationalDatabaseConnectorConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerConnectorConfig.class);

    protected static final int DEFAULT_PORT = 1433;
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

    public static final Field HOSTNAME = Field.create(DATABASE_CONFIG_PREFIX + JdbcConfiguration.HOSTNAME)
            .withDisplayName("Hostname")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withValidation(Field::isRequired)
            .withDescription("Resolvable hostname or IP address of the SQL Server database server.");

    public static final Field PORT = Field.create(DATABASE_CONFIG_PREFIX + JdbcConfiguration.PORT)
            .withDisplayName("Port")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withDefault(DEFAULT_PORT)
            .withImportance(Importance.HIGH)
            .withValidation(Field::isInteger)
            .withDescription("Port of the SQL Server database server.");

    public static final Field USER = Field.create(DATABASE_CONFIG_PREFIX + JdbcConfiguration.USER)
            .withDisplayName("User")
            .withType(Type.STRING)
            .withWidth(Width.SHORT)
            .withImportance(Importance.HIGH)
            .withValidation(Field::isRequired)
            .withDescription("Name of the SQL Server database user to be used when connecting to the database.");

    public static final Field PASSWORD = Field.create(DATABASE_CONFIG_PREFIX + JdbcConfiguration.PASSWORD)
            .withDisplayName("Password")
            .withType(Type.PASSWORD)
            .withWidth(Width.SHORT)
            .withImportance(Importance.HIGH)
            .withDescription("Password of the SQL Server database user to be used when connecting to the database.");

    public static final Field SERVER_NAME = RelationalDatabaseConnectorConfig.SERVER_NAME
            .withValidation(CommonConnectorConfig::validateServerNameIsDifferentFromHistoryTopicName);

    public static final Field DATABASE_NAME = Field.create(DATABASE_CONFIG_PREFIX + JdbcConfiguration.DATABASE)
            .withDisplayName("Database name")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withValidation(Field::isRequired)
            .withDescription("The name of the database the connector should be monitoring. When working with a "
                    + "multi-tenant set-up, must be set to the CDB name.");

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
                    + "Options include: Any valid Java ZoneId");

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

    /**
     * The set of {@link Field}s defined as part of this configuration.
     */
    public static Field.Set ALL_FIELDS = Field.setOf(
            SERVER_NAME,
            DATABASE_NAME,
            HOSTNAME,
            PORT,
            USER,
            PASSWORD,
            SNAPSHOT_MODE,
            SERVER_TIMEZONE,
            RelationalDatabaseConnectorConfig.SNAPSHOT_LOCK_TIMEOUT_MS,
            RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE,
            HistorizedRelationalDatabaseConnectorConfig.DATABASE_HISTORY,
            RelationalDatabaseConnectorConfig.TABLE_WHITELIST,
            RelationalDatabaseConnectorConfig.TABLE_BLACKLIST,
            RelationalDatabaseConnectorConfig.TABLE_IGNORE_BUILTIN,
            RelationalDatabaseConnectorConfig.COLUMN_BLACKLIST,
            RelationalDatabaseConnectorConfig.MSG_KEY_COLUMNS,
            RelationalDatabaseConnectorConfig.DECIMAL_HANDLING_MODE,
            RelationalDatabaseConnectorConfig.TIME_PRECISION_MODE,
            CommonConnectorConfig.POLL_INTERVAL_MS,
            CommonConnectorConfig.MAX_BATCH_SIZE,
            CommonConnectorConfig.MAX_QUEUE_SIZE,
            CommonConnectorConfig.SNAPSHOT_DELAY_MS,
            CommonConnectorConfig.SNAPSHOT_FETCH_SIZE,
            CommonConnectorConfig.TOMBSTONES_ON_DELETE,
            CommonConnectorConfig.PROVIDE_TRANSACTION_METADATA,
            Heartbeat.HEARTBEAT_INTERVAL, Heartbeat.HEARTBEAT_TOPICS_PREFIX,
            CommonConnectorConfig.SOURCE_STRUCT_MAKER_VERSION,
            CommonConnectorConfig.EVENT_PROCESSING_FAILURE_HANDLING_MODE);

    public static ConfigDef configDef() {
        ConfigDef config = new ConfigDef();

        Field.group(config, "SQL Server", SERVER_NAME, DATABASE_NAME, HOSTNAME, PORT,
                USER, PASSWORD, SNAPSHOT_MODE, SERVER_TIMEZONE);
        Field.group(config, "History Storage", KafkaDatabaseHistory.BOOTSTRAP_SERVERS,
                KafkaDatabaseHistory.TOPIC, KafkaDatabaseHistory.RECOVERY_POLL_ATTEMPTS,
                KafkaDatabaseHistory.RECOVERY_POLL_INTERVAL_MS, HistorizedRelationalDatabaseConnectorConfig.DATABASE_HISTORY);
        Field.group(config, "Events", RelationalDatabaseConnectorConfig.TABLE_WHITELIST,
                RelationalDatabaseConnectorConfig.TABLE_BLACKLIST,
                RelationalDatabaseConnectorConfig.COLUMN_BLACKLIST,
                RelationalDatabaseConnectorConfig.MSG_KEY_COLUMNS,
                RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE,
                RelationalDatabaseConnectorConfig.TABLE_IGNORE_BUILTIN,
                Heartbeat.HEARTBEAT_INTERVAL, Heartbeat.HEARTBEAT_TOPICS_PREFIX,
                CommonConnectorConfig.SOURCE_STRUCT_MAKER_VERSION,
                CommonConnectorConfig.TOMBSTONES_ON_DELETE,
                CommonConnectorConfig.PROVIDE_TRANSACTION_METADATA,
                CommonConnectorConfig.EVENT_PROCESSING_FAILURE_HANDLING_MODE);
        Field.group(config, "Connector", CommonConnectorConfig.POLL_INTERVAL_MS, CommonConnectorConfig.MAX_BATCH_SIZE,
                CommonConnectorConfig.MAX_QUEUE_SIZE, CommonConnectorConfig.SNAPSHOT_DELAY_MS, CommonConnectorConfig.SNAPSHOT_FETCH_SIZE,
                RelationalDatabaseConnectorConfig.DECIMAL_HANDLING_MODE, RelationalDatabaseConnectorConfig.TIME_PRECISION_MODE,
                RelationalDatabaseConnectorConfig.SNAPSHOT_LOCK_TIMEOUT_MS);

        return config;
    }

    private final String databaseName;
    private final SnapshotMode snapshotMode;
    private final SnapshotIsolationMode snapshotIsolationMode;
    private final ColumnNameFilter columnFilter;
    private final boolean readOnlyDatabaseConnection;

    public SqlServerConnectorConfig(Configuration config) {
        super(config, config.getString(SERVER_NAME), new SystemTablesPredicate(), x -> x.schema() + "." + x.table(), true);

        this.databaseName = config.getString(DATABASE_NAME);
        this.snapshotMode = SnapshotMode.parse(config.getString(SNAPSHOT_MODE), SNAPSHOT_MODE.defaultValueAsString());

        this.columnFilter = getColumnNameFilter(config.getString(RelationalDatabaseConnectorConfig.COLUMN_BLACKLIST));
        this.readOnlyDatabaseConnection = READ_ONLY_INTENT.equals(config.getString(APPLICATION_INTENT_KEY));
        if (readOnlyDatabaseConnection) {
            this.snapshotIsolationMode = SnapshotIsolationMode.SNAPSHOT;
            LOGGER.info("JDBC connection has set applicationIntent = ReadOnly, switching snapshot isolation mode to {}", SnapshotIsolationMode.SNAPSHOT.name());
        }
        else {
            this.snapshotIsolationMode = SnapshotIsolationMode.parse(config.getString(SNAPSHOT_ISOLATION_MODE), SNAPSHOT_ISOLATION_MODE.defaultValueAsString());
        }
    }

    private static ColumnNameFilter getColumnNameFilter(String excludedColumnPatterns) {
        return new ColumnNameFilter() {

            Predicate<ColumnId> delegate = Predicates.excludes(excludedColumnPatterns, ColumnId::toString);

            @Override
            public boolean matches(String catalogName, String schemaName, String tableName, String columnName) {
                // ignore database name as it's not relevant here
                return delegate.test(new ColumnId(new TableId(null, schemaName, tableName), columnName));
            }
        };
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public SnapshotIsolationMode getSnapshotIsolationMode() {
        return this.snapshotIsolationMode;
    }

    public SnapshotMode getSnapshotMode() {
        return snapshotMode;
    }

    public ColumnNameFilter getColumnFilter() {
        return columnFilter;
    }

    public boolean isReadOnlyDatabaseConnection() {
        return readOnlyDatabaseConnection;
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
            return !(t.schema().toLowerCase().equals("cdc") ||
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
}
