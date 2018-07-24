/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.document.Document;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.history.DatabaseHistory;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.KafkaDatabaseHistory;

/**
 * The list of configuration options for SQL Server connector
 *
 * @author Jiri Pechanec
 */
public class SqlServerConnectorConfig extends RelationalDatabaseConnectorConfig {

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
        INITIAL_SCHEMA_ONLY("initial_schema_only", false);

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
                if (option.getValue().equalsIgnoreCase(value)) return option;
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
     * The set of predefined Snapshot Locking Mode options.
     */
    public static enum SnapshotLockingMode implements EnumeratedValue {

        /**
         * This mode will block all reads and writes for the entire duration of the snapshot.
         *
         * The connector will execute {@code SELECT * FROM .. WITH (TABLOCKX)}
         */
        EXCLUSIVE("exclusive"),

        /**
         * This mode will avoid using ANY table locks during the snapshot process.  This mode can only be used with SnapShotMode
         * set to schema_only or schema_only_recovery.
         */
        NONE("none");

        private final String value;

        private SnapshotLockingMode(String value) {
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
        public static SnapshotLockingMode parse(String value) {
            if (value == null) return null;
            value = value.trim();
            for (SnapshotLockingMode option : SnapshotLockingMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) return option;
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
        public static SnapshotLockingMode parse(String value, String defaultValue) {
            SnapshotLockingMode mode = parse(value);
            if (mode == null && defaultValue != null) mode = parse(defaultValue);
            return mode;
        }
    }

    public static final Field LOGICAL_NAME = Field.create("database.server.name")
            .withDisplayName("Namespace")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withValidation(Field::isRequired)
            .withValidation(Field::isRequired, CommonConnectorConfig::validateServerNameIsDifferentFromHistoryTopicName)
            .withDescription("Unique name that identifies the database server and all recorded offsets, and"
                    + "that is used as a prefix for all schemas and topics. "
                    + "Each distinct SQL Server installation should have a separate namespace and monitored by "
                    + "at most one Debezium connector.");

    public static final Field DATABASE_NAME = Field.create(DATABASE_CONFIG_PREFIX + JdbcConfiguration.DATABASE)
            .withDisplayName("Database name")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withValidation(Field::isRequired)
            .withDescription("The name of the database the connector should be monitoring. When working with a "
                    + "multi-tenant set-up, must be set to the CDB name.");

    /**
     * The database history class is hidden in the {@link #configDef()} since that is designed to work with a user interface,
     * and in these situations using Kafka is the only way to go.
     */
    public static final Field DATABASE_HISTORY = Field.create("database.history")
            .withDisplayName("Database history class")
            .withType(Type.CLASS)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withInvisibleRecommender()
            .withDescription("The name of the DatabaseHistory class that should be used to store and recover database schema changes. "
                    + "The configuration properties for the history are prefixed with the '"
                    + DatabaseHistory.CONFIGURATION_FIELD_PREFIX_STRING + "' string.")
            .withDefault(KafkaDatabaseHistory.class.getName());

    public static final Field SNAPSHOT_MODE = Field.create("snapshot.mode")
            .withDisplayName("Snapshot mode")
            .withEnum(SnapshotMode.class, SnapshotMode.INITIAL)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("The criteria for running a snapshot upon startup of the connector. "
                    + "Options include: "
                    + "'initial' (the default) to specify the connector should run a snapshot only when no offsets are available for the logical server name; "
                    + "'initial_schema_only' to specify the connector should run a snapshot of the schema when no offsets are available for the logical server name. ");

    public static final Field SNAPSHOT_LOCKING_MODE = Field.create("snapshot.locking.mode")
            .withDisplayName("Snapshot locking mode")
            .withEnum(SnapshotLockingMode.class, SnapshotLockingMode.NONE)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Controls how long the connector locks the montiored tables for snapshot execution. The default is '" + SnapshotLockingMode.NONE.getValue() + "', "
                + "which means that the connector does not hold any locks for all monitored tables."
                + "Using a value of '" + SnapshotLockingMode.EXCLUSIVE.getValue() + "' ensures that the connector holds the exlusive lock (and thus prevents any reads and updates) for all monitored tables.");

    /**
     * The set of {@link Field}s defined as part of this configuration.
     */
    public static Field.Set ALL_FIELDS = Field.setOf(
            LOGICAL_NAME,
            DATABASE_NAME,
            SNAPSHOT_MODE,
            RelationalDatabaseConnectorConfig.TABLE_WHITELIST,
            RelationalDatabaseConnectorConfig.TABLE_BLACKLIST,
            RelationalDatabaseConnectorConfig.TABLE_IGNORE_BUILTIN,
            CommonConnectorConfig.POLL_INTERVAL_MS,
            CommonConnectorConfig.MAX_BATCH_SIZE,
            CommonConnectorConfig.MAX_QUEUE_SIZE,
            Heartbeat.HEARTBEAT_INTERVAL, Heartbeat.HEARTBEAT_TOPICS_PREFIX
    );

    public static ConfigDef configDef() {
        ConfigDef config = new ConfigDef();

        Field.group(config, "SQL Server", LOGICAL_NAME, DATABASE_NAME, SNAPSHOT_MODE);
        Field.group(config, "Events", RelationalDatabaseConnectorConfig.TABLE_WHITELIST,
                RelationalDatabaseConnectorConfig.TABLE_BLACKLIST,
                RelationalDatabaseConnectorConfig.TABLE_IGNORE_BUILTIN,
                Heartbeat.HEARTBEAT_INTERVAL, Heartbeat.HEARTBEAT_TOPICS_PREFIX
        );
        Field.group(config, "Connector", CommonConnectorConfig.POLL_INTERVAL_MS, CommonConnectorConfig.MAX_BATCH_SIZE, CommonConnectorConfig.MAX_QUEUE_SIZE);

        return config;
    }

    private final String databaseName;
    private final SnapshotMode snapshotMode;
    private final SnapshotLockingMode snapshotLockingMode;

    public SqlServerConnectorConfig(Configuration config) {
        super(config, config.getString(LOGICAL_NAME), new SystemTablesPredicate());

        this.databaseName = config.getString(DATABASE_NAME);
        this.snapshotMode = SnapshotMode.parse(config.getString(SNAPSHOT_MODE), SNAPSHOT_MODE.defaultValueAsString());
        this.snapshotLockingMode = SnapshotLockingMode.parse(config.getString(SNAPSHOT_LOCKING_MODE), SNAPSHOT_LOCKING_MODE.defaultValueAsString());
    }

    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Returns a configured (but not yet started) instance of the database history.
     */
    @Override
    public DatabaseHistory getDatabaseHistory() {
        Configuration config = getConfig();

        DatabaseHistory databaseHistory = config.getInstance(SqlServerConnectorConfig.DATABASE_HISTORY, DatabaseHistory.class);
        if (databaseHistory == null) {
            throw new ConnectException("Unable to instantiate the database history class " +
                    config.getString(SqlServerConnectorConfig.DATABASE_HISTORY));
        }

        // Do not remove the prefix from the subset of config properties ...
        Configuration dbHistoryConfig = config.subset(DatabaseHistory.CONFIGURATION_FIELD_PREFIX_STRING, false)
                                              .edit()
                                              .withDefault(DatabaseHistory.NAME, getLogicalName() + "-dbhistory")
                                              .build();

        HistoryRecordComparator historyComparator = new HistoryRecordComparator() {
            @Override
            protected boolean isPositionAtOrBefore(Document recorded, Document desired) {
                return Lsn.valueOf(recorded.getString(SourceInfo.CHANGE_LSN_KEY)).compareTo(Lsn.valueOf(desired.getString(SourceInfo.CHANGE_LSN_KEY))) < 1;
            }
        };
        databaseHistory.configure(dbHistoryConfig, historyComparator); // validates

        return databaseHistory;
    }

    public SnapshotLockingMode getSnapshotLockingMode() {
        return this.snapshotLockingMode;
    }

    public SnapshotMode getSnapshotMode() {
        return snapshotMode;
    }

    private static class SystemTablesPredicate implements TableFilter {

        @Override
        public boolean isIncluded(TableId t) {
            return !(t.schema().toLowerCase().equals("cdc") ||
                    t.schema().toLowerCase().equals("sys") ||
                    t.table().toLowerCase().equals("systranschemas"));
            }
    }
}
