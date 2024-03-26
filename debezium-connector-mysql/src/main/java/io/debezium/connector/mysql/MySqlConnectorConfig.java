/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

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
import io.debezium.config.Field.ValidationOutput;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.connector.mysql.strategy.ConnectorAdapter;
import io.debezium.connector.mysql.strategy.mariadb.MariaDbConnectorAdapter;
import io.debezium.connector.mysql.strategy.mariadb.hybrid.MariaDbHybridConnectorAdapter;
import io.debezium.connector.mysql.strategy.mysql.MySqlConnectorAdapter;
import io.debezium.function.Predicates;
import io.debezium.jdbc.JdbcValueConverters.BigIntUnsignedMode;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.schema.DefaultTopicNamingStrategy;
import io.debezium.util.Collect;
import io.debezium.util.Strings;

/**
 * The configuration properties.
 */
public class MySqlConnectorConfig extends HistorizedRelationalDatabaseConnectorConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlConnectorConfig.class);

    /**
     * It is not possible to test disabled global locking locally as regular MySQL build always
     * provides global locking. So to bypass this limitation it is necessary to provide a backdoor
     * to connector to disable it on its own.
     */
    static final String TEST_DISABLE_GLOBAL_LOCKING = "test.disable.global.locking";

    /**
     * The set of predefined BigIntUnsignedHandlingMode options or aliases.
     */
    public enum BigIntUnsignedHandlingMode implements EnumeratedValue {
        /**
         * Represent {@code BIGINT UNSIGNED} values as precise {@link BigDecimal} values, which are
         * represented in change events in a binary form. This is precise but difficult to use.
         */
        PRECISE("precise"),

        /**
         * Represent {@code BIGINT UNSIGNED} values as precise {@code long} values. This may be less precise
         * but is far easier to use.
         */
        LONG("long");

        private final String value;

        BigIntUnsignedHandlingMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        public BigIntUnsignedMode asBigIntUnsignedMode() {
            switch (this) {
                case LONG:
                    return BigIntUnsignedMode.LONG;
                case PRECISE:
                default:
                    return BigIntUnsignedMode.PRECISE;
            }
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static BigIntUnsignedHandlingMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (BigIntUnsignedHandlingMode option : BigIntUnsignedHandlingMode.values()) {
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
        public static BigIntUnsignedHandlingMode parse(String value, String defaultValue) {
            BigIntUnsignedHandlingMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    /**
     * The set of predefined SnapshotMode options or aliases.
     */
    public enum SnapshotMode implements EnumeratedValue {

        /**
         * Performs a snapshot of data and schema upon each connector start.
         */
        ALWAYS("always"),

        /**
         * Perform a snapshot when it is needed.
         */
        WHEN_NEEDED("when_needed"),

        /**
         * Perform a snapshot only upon initial startup of a connector.
         */
        INITIAL("initial"),

        /**
         * Perform a snapshot of only the database schemas (without data) and then begin reading the binlog.
         * This should be used with care, but it is very useful when the change event consumers need only the changes
         * from the point in time the snapshot is made (and doesn't care about any state or changes prior to this point).
         *
         * @deprecated to be removed in Debezium 3.0, replaced by {{@link #NO_DATA}}
         */
        SCHEMA_ONLY("schema_only"),

        /**
         * Perform a snapshot of only the database schemas (without data) and then begin reading the binlog.
         * This should be used with care, but it is very useful when the change event consumers need only the changes
         * from the point in time the snapshot is made (and doesn't care about any state or changes prior to this point).
         */
        NO_DATA("no_data"),

        /**
         * Perform a snapshot of only the database schemas (without data) and then begin reading the binlog at the current binlog position.
         * This can be used for recovery only if the connector has existing offsets and the schema.history.internal.kafka.topic does not exist (deleted).
         * This recovery option should be used with care as it assumes there have been no schema changes since the connector last stopped,
         * otherwise some events during the gap may be processed with an incorrect schema and corrupted.
         *
         * @deprecated to be removed in Debezium 3.0, replaced by {{@link #RECOVERY}}
         */
        SCHEMA_ONLY_RECOVERY("schema_only_recovery"),

        /**
         * Perform a snapshot of only the database schemas (without data) and then begin reading the binlog at the current binlog position.
         * This can be used for recovery only if the connector has existing offsets and the schema.history.internal.kafka.topic does not exist (deleted).
         * This recovery option should be used with care as it assumes there have been no schema changes since the connector last stopped,
         * otherwise some events during the gap may be processed with an incorrect schema and corrupted.
         */
        RECOVERY("recovery"),

        /**
         * Never perform a snapshot and only read the binlog. This assumes the binlog contains all the history of those
         * databases and tables that will be captured.
         */
        NEVER("never"),

        /**
         * Perform a snapshot and then stop before attempting to read the binlog.
         */
        INITIAL_ONLY("initial_only"),

        /**
         * Allows control over snapshots by setting connectors properties prefixed with 'snapshot.mode.configuration.based'.
         */
        CONFIGURATION_BASED("configuration_based"),

        /**
         * Inject a custom snapshotter, which allows for more control over snapshots.
         */
        CUSTOM("custom");

        private final String value;

        SnapshotMode(String value) {
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

    public enum SnapshotNewTables implements EnumeratedValue {
        /**
         * Do not snapshot new tables
         */
        OFF("off"),

        /**
         * Snapshot new tables in parallel to normal binlog reading.
         */
        PARALLEL("parallel");

        private final String value;

        SnapshotNewTables(String value) {
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
        public static SnapshotNewTables parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (SnapshotNewTables option : SnapshotNewTables.values()) {
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
        public static SnapshotNewTables parse(String value, String defaultValue) {
            SnapshotNewTables snapshotNewTables = parse(value);
            if (snapshotNewTables == null && defaultValue != null) {
                snapshotNewTables = parse(defaultValue);
            }
            return snapshotNewTables;
        }
    }

    /**
     * The set of predefined Snapshot Locking Mode options.
     */
    public enum SnapshotLockingMode implements EnumeratedValue {

        /**
         * This mode will block all writes for the entire duration of the snapshot.
         *
         * Replaces deprecated configuration option snapshot.locking.minimal with a value of false.
         */
        EXTENDED("extended"),

        /**
         * The connector holds the global read lock for just the initial portion of the snapshot while the connector reads the database
         * schemas and other metadata. The remaining work in a snapshot involves selecting all rows from each table, and this can be done
         * in a consistent fashion using the REPEATABLE READ transaction even when the global read lock is no longer held and while other
         * MySQL clients are updating the database.
         *
         * Replaces deprecated configuration option snapshot.locking.minimal with a value of true.
         */
        MINIMAL("minimal"),

        /**
         * The connector holds a (Percona-specific) backup lock for just the initial portion of the snapshot while the connector
         * reads the database schemas and other metadata. This lock will only block DDL and DML on non-transactional tables
         * (MyISAM etc.). The remaining work in a snapshot involves selecting all rows from each table, and this can be done in a
         * consistent fashion using the REPEATABLE READ transaction even when the global read lock is no longer held and while other
         * MySQL clients are updating the database.
         */
        MINIMAL_PERCONA("minimal_percona"),

        /**
         * This mode will avoid using ANY table locks during the snapshot process.  This mode can only be used with SnapShotMode
         * set to schema_only or schema_only_recovery.
         */
        NONE("none"),

        /**
         * Inject a custom mode, which allows for more control over snapshot locking.
         */
        CUSTOM("custom");

        private final String value;

        SnapshotLockingMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        public boolean usesMinimalLocking() {
            return value.equals(MINIMAL.value) || value.equals(MINIMAL_PERCONA.value);
        }

        public boolean usesLocking() {
            return !value.equals(NONE.value);
        }

        public boolean flushResetsIsolationLevel() {
            return !value.equals(MINIMAL_PERCONA.value);
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static SnapshotLockingMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (SnapshotLockingMode option : SnapshotLockingMode.values()) {
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
        public static SnapshotLockingMode parse(String value, String defaultValue) {
            SnapshotLockingMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    /**
     * The set of predefined SecureConnectionMode options or aliases.
     */
    public enum SecureConnectionMode implements EnumeratedValue {
        /**
         * Establish an unencrypted connection.
         */
        DISABLED("disabled"),

        /**
         * Establish a secure (encrypted) connection if the server supports secure connections.
         * Fall back to an unencrypted connection otherwise.
         */
        PREFERRED("preferred"),
        /**
         * Establish a secure connection if the server supports secure connections.
         * The connection attempt fails if a secure connection cannot be established.
         */
        REQUIRED("required"),
        /**
         * Like REQUIRED, but additionally verify the server TLS certificate against the configured Certificate Authority
         * (CA) certificates. The connection attempt fails if no valid matching CA certificates are found.
         */
        VERIFY_CA("verify_ca"),
        /**
         * Like VERIFY_CA, but additionally verify that the server certificate matches the host to which the connection is
         * attempted.
         */
        VERIFY_IDENTITY("verify_identity");

        private final String value;

        SecureConnectionMode(String value) {
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
        public static SecureConnectionMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (SecureConnectionMode option : SecureConnectionMode.values()) {
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
        public static SecureConnectionMode parse(String value, String defaultValue) {
            SecureConnectionMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    /**
     * Set of predefined connector adapter modes.
     */
    public enum ConnectorAdapterMode implements EnumeratedValue {
        /**
         * Expects the target database to be MySQL using the MySQL driver.
         * This should also be used if the target database is MySQL compliant but isn't MariaDB.
         */
        MYSQL("mysql") {
            @Override
            protected ConnectorAdapter getAdapter(MySqlConnectorConfig connectorConfig) {
                LOGGER.info("Using " + MySqlConnectorAdapter.class.getName());
                return new MySqlConnectorAdapter(connectorConfig);
            }
        },

        /**
         * Expects the target database to be MariaDB using the MariaDB driver.
         */
        MARIADB("mariadb") {
            @Override
            protected ConnectorAdapter getAdapter(MySqlConnectorConfig connectorConfig) {
                LOGGER.info("Using " + MariaDbConnectorAdapter.class.getName());
                return new MariaDbConnectorAdapter(connectorConfig);
            }
        },

        /**
         * Expects the target database to be MariaDB but uses the MySQL driver.
         */
        MARIADB_HYBRID("mariadb-hybrid") {
            @Override
            protected ConnectorAdapter getAdapter(MySqlConnectorConfig connectorConfig) {
                LOGGER.info("Using " + MariaDbHybridConnectorAdapter.class.getName());
                return new MariaDbHybridConnectorAdapter(connectorConfig);
            }
        };

        private final String value;

        protected abstract ConnectorAdapter getAdapter(MySqlConnectorConfig connectorConfig);

        ConnectorAdapterMode(String value) {
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
        public static ConnectorAdapterMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (ConnectorAdapterMode option : ConnectorAdapterMode.values()) {
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
        public static ConnectorAdapterMode parse(String value, String defaultValue) {
            ConnectorAdapterMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    /**
     * {@link Integer#MIN_VALUE Minimum value} used for fetch size hint.
     * See <a href="https://issues.jboss.org/browse/DBZ-94">DBZ-94</a> for details.
     *
     * This fetch size is not valid for MariaDB and per <a href="https://jira.mariadb.org/browse/CONJ-977">CONJ-997</a>
     * they believe that the MySQL implementation for this is not according to the spec. Starting
     * with MariaDB driver's 3.x+, this value cannot be negative. See the configuration method
     * {@link #resolveDefaultFetchSize(Configuration)} for more details when MariaDB is enabled.
     */
    protected static final int DEFAULT_SNAPSHOT_FETCH_SIZE = Integer.MIN_VALUE;

    protected static final int DEFAULT_PORT = 3306;

    /**
     * Default size of the binlog buffer used for examining transactions and
     * deciding whether to propagate them or not. A size of 0 disables the buffer,
     * all events will be passed on directly as they are passed by the binlog
     * client.
     */
    private static final int DEFAULT_BINLOG_BUFFER_SIZE = 0;

    public static final Field PORT = RelationalDatabaseConnectorConfig.PORT
            .withDefault(DEFAULT_PORT);

    public static final Field ON_CONNECT_STATEMENTS = Field.create("database.initial.statements")
            .withDisplayName("Initial statements")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 4))
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withDescription(
                    "A semicolon separated list of SQL statements to be executed when a JDBC connection (not binlog reading connection) to the database is established. "
                            + "Note that the connector may establish JDBC connections at its own discretion, so this should typically be used for configuration of session parameters only, "
                            + "but not for executing DML statements. Use doubled semicolon (';;') to use a semicolon as a character and not as a delimiter.");

    public static final Field SERVER_ID = Field.create("database.server.id")
            .withDisplayName("Cluster ID")
            .withType(Type.LONG)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 1))
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .required()
            .withValidation(Field::isPositiveLong)
            .withDescription("A numeric ID of this database client, which must be unique across all "
                    + "currently-running database processes in the cluster. This connector joins the "
                    + "MySQL database cluster as another server (with this unique ID) so it can read "
                    + "the binlog.");

    public static final Field SERVER_ID_OFFSET = Field.create("database.server.id.offset")
            .withDisplayName("Cluster ID offset")
            .withType(Type.LONG)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 0))
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withDefault(10000L)
            .withDescription("Only relevant if parallel snapshotting is configured. During "
                    + "parallel snapshotting, multiple (4) connections open to the database "
                    + "client, and they each need their own unique connection ID. This offset is "
                    + "used to generate those IDs from the base configured cluster ID.");

    public static final Field SSL_MODE = Field.create("database.ssl.mode")
            .withDisplayName("SSL mode")
            .withEnum(SecureConnectionMode.class, SecureConnectionMode.PREFERRED)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_SSL, 0))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDescription("Whether to use an encrypted connection to MySQL. Options include: "
                    + "'disabled' to use an unencrypted connection; "
                    + "'preferred' (the default) to establish a secure (encrypted) connection if the server supports secure connections, "
                    + "but fall back to an unencrypted connection otherwise; "
                    + "'required' to use a secure (encrypted) connection, and fail if one cannot be established; "
                    + "'verify_ca' like 'required' but additionally verify the server TLS certificate against the configured Certificate Authority "
                    + "(CA) certificates, or fail if no valid matching CA certificates are found; or"
                    + "'verify_identity' like 'verify_ca' but additionally verify that the server certificate matches the host to which the connection is attempted.");

    public static final Field SSL_KEYSTORE = Field.create("database.ssl.keystore")
            .withDisplayName("SSL Keystore")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_SSL, 1))
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDescription("The location of the key store file. "
                    + "This is optional and can be used for two-way authentication between the client and the MySQL Server.");

    public static final Field SSL_KEYSTORE_PASSWORD = Field.create("database.ssl.keystore.password")
            .withDisplayName("SSL Keystore Password")
            .withType(Type.PASSWORD)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_SSL, 2))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDescription("The password for the key store file. "
                    + "This is optional and only needed if 'database.ssl.keystore' is configured.");

    public static final Field SSL_TRUSTSTORE = Field.create("database.ssl.truststore")
            .withDisplayName("SSL Truststore")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_SSL, 3))
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDescription("The location of the trust store file for the server certificate verification.");

    public static final Field SSL_TRUSTSTORE_PASSWORD = Field.create("database.ssl.truststore.password")
            .withDisplayName("SSL Truststore Password")
            .withType(Type.PASSWORD)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_SSL, 4))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDescription("The password for the trust store file. "
                    + "Used to check the integrity of the truststore, and unlock the truststore.");

    public static final Field TABLES_IGNORE_BUILTIN = RelationalDatabaseConnectorConfig.TABLE_IGNORE_BUILTIN
            .withDependents(DATABASE_INCLUDE_LIST_NAME);

    public static final Field JDBC_DRIVER = Field.create(DATABASE_CONFIG_PREFIX + "jdbc.driver")
            .withDisplayName("JDBC Driver Class Name")
            .withType(Type.CLASS)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 41))
            .withWidth(Width.MEDIUM)
            .withDefault(com.mysql.cj.jdbc.Driver.class.getName())
            .withImportance(Importance.LOW)
            .withValidation(Field::isClassName)
            .withDescription("JDBC Driver class name used to connect to the MySQL database server.");

    public static final Field JDBC_PROTOCOL = Field.create(DATABASE_CONFIG_PREFIX + "protocol")
            .withDisplayName("JDBC Protocol")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 42))
            .withWidth(Width.MEDIUM)
            .withDefault("jdbc:mysql")
            .withImportance(Importance.LOW)
            .withDescription("JDBC protocol to use with the driver.");

    /**
     * A comma-separated list of regular expressions that match source UUIDs in the GTID set used to find the binlog
     * position in the MySQL server. Only the GTID ranges that have sources matching one of these include patterns will
     * be used.
     * Must not be used with {@link #GTID_SOURCE_EXCLUDES}.
     */
    public static final Field GTID_SOURCE_INCLUDES = Field.create("gtid.source.includes")
            .withDisplayName("Include GTID sources")
            .withType(Type.LIST)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 24))
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withDependents(TABLE_INCLUDE_LIST_NAME)
            .withDescription("The source UUIDs used to include GTID ranges when determine the starting position in the MySQL server's binlog.");

    /**
     * A comma-separated list of regular expressions that match source UUIDs in the GTID set used to find the binlog
     * position in the MySQL server. Only the GTID ranges that have sources matching none of these exclude patterns will
     * be used.
     * Must not be used with {@link #GTID_SOURCE_INCLUDES}.
     */
    public static final Field GTID_SOURCE_EXCLUDES = Field.create("gtid.source.excludes")
            .withDisplayName("Exclude GTID sources")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 25))
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withValidation(MySqlConnectorConfig::validateGtidSetExcludes)
            .withInvisibleRecommender()
            .withDescription("The source UUIDs used to exclude GTID ranges when determine the starting position in the MySQL server's binlog.");

    /**
     * If set to true, we will only produce DML events into Kafka for transactions that were written on MySQL servers
     * with UUIDs matching the filters defined by the {@link #GTID_SOURCE_INCLUDES} or {@link #GTID_SOURCE_EXCLUDES}
     * configuration options, if they are specified.
     *
     * Defaults to true.
     *
     * When true, either {@link #GTID_SOURCE_INCLUDES} or {@link #GTID_SOURCE_EXCLUDES} must be set.
     */
    public static final Field GTID_SOURCE_FILTER_DML_EVENTS = Field.create("gtid.source.filter.dml.events")
            .withDisplayName("Filter DML events")
            .withType(Type.BOOLEAN)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 23))
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(true)
            .withDescription(
                    "If set to true, we will only produce DML events into Kafka for transactions that were written on mysql servers with UUIDs matching the filters defined by the gtid.source.includes or gtid.source.excludes configuration options, if they are specified.");

    public static final Field CONNECTION_TIMEOUT_MS = Field.create("connect.timeout.ms")
            .withDisplayName("Connection Timeout (ms)")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 1))
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription(
                    "Maximum time to wait after trying to connect to the database before timing out, given in milliseconds. Defaults to 30 seconds (30,000 ms).")
            .withDefault(30 * 1000)
            .withValidation(Field::isPositiveInteger);

    public static final Field KEEP_ALIVE = Field.create("connect.keep.alive")
            .withDisplayName("Keep connection alive (true/false)")
            .withType(Type.BOOLEAN)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 2))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Whether a separate thread should be used to ensure the connection is kept alive.")
            .withDefault(true)
            .withValidation(Field::isBoolean);

    public static final Field KEEP_ALIVE_INTERVAL_MS = Field.create("connect.keep.alive.interval.ms")
            .withDisplayName("Keep alive interval (ms)")
            .withType(Type.LONG)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 3))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Interval for connection checking if keep alive thread is used, given in milliseconds Defaults to 1 minute (60,000 ms).")
            .withDefault(Duration.ofMinutes(1).toMillis())
            .withValidation(Field::isPositiveInteger);

    public static final Field ROW_COUNT_FOR_STREAMING_RESULT_SETS = Field.create("min.row.count.to.stream.results")
            .withDisplayName("Stream result set of size")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 2))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDescription("The number of rows a table must contain to stream results rather than pull "
                    + "all into memory during snapshots. Defaults to 1,000. Use 0 to stream all results "
                    + "and completely avoid checking the size of each table.")
            .withDefault(1_000)
            .withValidation(Field::isNonNegativeLong);

    public static final Field BUFFER_SIZE_FOR_BINLOG_READER = Field.create("binlog.buffer.size")
            .withDisplayName("Binlog reader buffer size")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 3))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDescription("The size of a look-ahead buffer used by the  binlog reader to decide whether "
                    + "the transaction in progress is going to be committed or rolled back. "
                    + "Use 0 to disable look-ahead buffering. "
                    + "Defaults to " + DEFAULT_BINLOG_BUFFER_SIZE + " (i.e. buffering is disabled).")
            .withDefault(DEFAULT_BINLOG_BUFFER_SIZE)
            .withValidation(Field::isNonNegativeInteger);

    public static final Field TOPIC_NAMING_STRATEGY = Field.create("topic.naming.strategy")
            .withDisplayName("Topic naming strategy class")
            .withType(Type.CLASS)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDescription("The name of the TopicNamingStrategy class that should be used to determine the topic name " +
                    "for data change, schema change, transaction, heartbeat event etc.")
            .withDefault(DefaultTopicNamingStrategy.class.getName());

    public static final Field INCLUDE_SQL_QUERY = Field.create("include.query")
            .withDisplayName("Include original SQL query with in change events")
            .withType(Type.BOOLEAN)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 0))
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Whether the connector should include the original SQL query that generated the change event. "
                    + "Note: This option requires MySQL be configured with the binlog_rows_query_log_events option set to ON. "
                    + "If using MariaDB, configure the binlog_annotate_row_events option must be set to ON. "
                    + "Query will not be present for events generated from snapshot. "
                    + "WARNING: Enabling this option may expose tables or fields explicitly excluded or masked by including the original SQL statement in the change event. "
                    + "For this reason the default value is 'false'.")
            .withDefault(false);

    public static final Field SNAPSHOT_MODE = Field.create(SNAPSHOT_MODE_PROPERTY_NAME)
            .withDisplayName("Snapshot mode")
            .withEnum(SnapshotMode.class, SnapshotMode.INITIAL)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 0))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("The criteria for running a snapshot upon startup of the connector. "
                    + "Select one of the following snapshot options: "
                    + "'when_needed': On startup, the connector runs a snapshot if one is needed.; "
                    + "'schema_only': If the connector does not detect any offsets for the logical server name, it runs a snapshot that captures only the schema (table structures), but not any table data. After the snapshot completes, the connector begins to stream changes from the binlog.; "
                    + "'schema_only_recovery': The connector performs a snapshot that captures only the database schema history. The connector then transitions back to streaming. Use this setting to restore a corrupted or lost database schema history topic. Do not use if the database schema was modified after the connector stopped.; "
                    + "'initial' (default): If the connector does not detect any offsets for the logical server name, it runs a snapshot that captures the current full state of the configured tables. After the snapshot completes, the connector begins to stream changes from the binlog.; "
                    + "'initial_only': The connector performs a snapshot as it does for the 'initial' option, but after the connector completes the snapshot, it stops, and does not stream changes from the binlog.; "
                    + "'never': The connector does not run a snapshot. Upon first startup, the connector immediately begins reading from the beginning of the binlog. "
                    + "The 'never' mode should be used with care, and only when the binlog is known to contain all history.");

    public static final Field SNAPSHOT_LOCKING_MODE = Field.create(SNAPSHOT_LOCKING_MODE_PROPERTY_NAME)
            .withDisplayName("Snapshot locking mode")
            .withEnum(SnapshotLockingMode.class, SnapshotLockingMode.MINIMAL)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 1))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Controls how long the connector holds onto the global read lock while it is performing a snapshot. The default is 'minimal', "
                    + "which means the connector holds the global read lock (and thus prevents any updates) for just the initial portion of the snapshot "
                    + "while the database schemas and other metadata are being read. The remaining work in a snapshot involves selecting all rows from "
                    + "each table, and this can be done using the snapshot process' REPEATABLE READ transaction even when the lock is no longer held and "
                    + "other operations are updating the database. However, in some cases it may be desirable to block all writes for the entire duration "
                    + "of the snapshot; in such cases set this property to 'extended'. Using a value of 'none' will prevent the connector from acquiring any "
                    + "table locks during the snapshot process. This mode can only be used in combination with snapshot.mode values of 'schema_only' or "
                    + "'schema_only_recovery' and is only safe to use if no schema changes are happening while the snapshot is taken.")
            .withValidation(MySqlConnectorConfig::validateSnapshotLockingMode);

    public static final Field SNAPSHOT_NEW_TABLES = Field.create("snapshot.new.tables")
            .withDisplayName("Snapshot newly added tables")
            .withEnum(SnapshotNewTables.class, SnapshotNewTables.OFF)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 4))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("BETA FEATURE: On connector restart, the connector will check if there have been any new tables added to the configuration, "
                    + "and snapshot them. There is presently only two options: "
                    + "'off': Default behavior. Do not snapshot new tables. "
                    + "'parallel': The snapshot of the new tables will occur in parallel to the continued binlog reading of the old tables. When the snapshot "
                    + "completes, an independent binlog reader will begin reading the events for the new tables until it catches up to present time. At this "
                    + "point, both old and new binlog readers will be momentarily halted and new binlog reader will start that will read the binlog for all "
                    + "configured tables. The parallel binlog reader will have a configured server id of 10000 + the primary binlog reader's server id.");

    public static final Field TIME_PRECISION_MODE = RelationalDatabaseConnectorConfig.TIME_PRECISION_MODE
            .withEnum(TemporalPrecisionMode.class, TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 26))
            .withValidation(MySqlConnectorConfig::validateTimePrecisionMode)
            .withDescription("Time, date and timestamps can be represented with different kinds of precisions, including: "
                    + "'adaptive_time_microseconds': the precision of date and timestamp values is based the database column's precision; but time fields always use microseconds precision; "
                    + "'connect': always represents time, date and timestamp values using Kafka Connect's built-in representations for Time, Date, and Timestamp, "
                    + "which uses millisecond precision regardless of the database columns' precision.");

    public static final Field BIGINT_UNSIGNED_HANDLING_MODE = Field.create("bigint.unsigned.handling.mode")
            .withDisplayName("BIGINT UNSIGNED Handling")
            .withEnum(BigIntUnsignedHandlingMode.class, BigIntUnsignedHandlingMode.LONG)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 27))
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Specify how BIGINT UNSIGNED columns should be represented in change events, including: "
                    + "'precise' uses java.math.BigDecimal to represent values, which are encoded in the change events using a binary representation and Kafka Connect's 'org.apache.kafka.connect.data.Decimal' type; "
                    + "'long' (the default) represents values using Java's 'long', which may not offer the precision but will be far easier to use in consumers.");

    public static final Field EVENT_DESERIALIZATION_FAILURE_HANDLING_MODE = Field.create("event.deserialization.failure.handling.mode")
            .withDisplayName("Event deserialization failure handling")
            .withEnum(EventProcessingFailureHandlingMode.class, EventProcessingFailureHandlingMode.FAIL)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 21))
            .withValidation(MySqlConnectorConfig::validateEventDeserializationFailureHandlingModeNotSet)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Specify how failures during deserialization of binlog events (i.e. when encountering a corrupted event) should be handled, including: "
                    + "'fail' (the default) an exception indicating the problematic event and its binlog position is raised, causing the connector to be stopped; "
                    + "'warn' the problematic event and its binlog position will be logged and the event will be skipped; "
                    + "'ignore' the problematic event will be skipped.");

    public static final Field INCONSISTENT_SCHEMA_HANDLING_MODE = Field.create("inconsistent.schema.handling.mode")
            .withDisplayName("Inconsistent schema failure handling")
            .withEnum(EventProcessingFailureHandlingMode.class, EventProcessingFailureHandlingMode.FAIL)
            .withGroup(Field.createGroupEntry(Field.Group.ADVANCED, 2))
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription(
                    "Specify how binlog events that belong to a table missing from internal schema representation (i.e. internal representation is not consistent with database) should be handled, including: "
                            + "'fail' (the default) an exception indicating the problematic event and its binlog position is raised, causing the connector to be stopped; "
                            + "'warn' the problematic event and its binlog position will be logged and the event will be skipped; "
                            + "'skip' the problematic event will be skipped.");

    public static final Field ENABLE_TIME_ADJUSTER = Field.create("enable.time.adjuster")
            .withDisplayName("Enable Time Adjuster")
            .withType(Type.BOOLEAN)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 22))
            .withDefault(true)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription(
                    "MySQL allows user to insert year value as either 2-digit or 4-digit. In case of two digit the value is automatically mapped into 1970 - 2069." +
                            "false - delegates the implicit conversion to the database" +
                            "true - (the default) Debezium makes the conversion");

    public static final Field READ_ONLY_CONNECTION = Field.create("read.only")
            .withDisplayName("Read only connection")
            .withType(Type.BOOLEAN)
            .withDefault(false)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Switched connector to use alternative methods to deliver signals to Debezium instead of writing to signaling table");

    public static final Field CONNECTOR_ADAPTER = Field.create("connector.adapter")
            .withDisplayName("Connection adapter to be used")
            .withEnum(ConnectorAdapterMode.class, ConnectorAdapterMode.MYSQL)
            .withGroup(Field.createGroupEntry(Field.Group.ADVANCED, 28))
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Specifies the connection adapter to be used");

    public static final Field SOURCE_INFO_STRUCT_MAKER = CommonConnectorConfig.SOURCE_INFO_STRUCT_MAKER
            .withDefault(MySqlSourceInfoStructMaker.class.getName());

    public static final Field STORE_ONLY_CAPTURED_DATABASES_DDL = HistorizedRelationalDatabaseConnectorConfig.STORE_ONLY_CAPTURED_DATABASES_DDL
            .withDefault(true);

    private static final ConfigDefinition CONFIG_DEFINITION = HistorizedRelationalDatabaseConnectorConfig.CONFIG_DEFINITION.edit()
            .name("MySQL")
            .excluding(
                    SCHEMA_INCLUDE_LIST,
                    SCHEMA_EXCLUDE_LIST,
                    RelationalDatabaseConnectorConfig.TIME_PRECISION_MODE,
                    RelationalDatabaseConnectorConfig.TABLE_IGNORE_BUILTIN,
                    HistorizedRelationalDatabaseConnectorConfig.STORE_ONLY_CAPTURED_DATABASES_DDL)
            .type(
                    HOSTNAME,
                    PORT,
                    USER,
                    PASSWORD,
                    ON_CONNECT_STATEMENTS,
                    SERVER_ID,
                    SERVER_ID_OFFSET,
                    SSL_MODE,
                    SSL_KEYSTORE,
                    SSL_KEYSTORE_PASSWORD,
                    SSL_TRUSTSTORE,
                    SSL_TRUSTSTORE_PASSWORD,
                    JDBC_DRIVER)
            .connector(
                    CONNECTION_TIMEOUT_MS,
                    KEEP_ALIVE,
                    KEEP_ALIVE_INTERVAL_MS,
                    SNAPSHOT_MODE,
                    SNAPSHOT_LOCKING_MODE,
                    SNAPSHOT_QUERY_MODE,
                    SNAPSHOT_QUERY_MODE_CUSTOM_NAME,
                    SNAPSHOT_NEW_TABLES,
                    BIGINT_UNSIGNED_HANDLING_MODE,
                    TIME_PRECISION_MODE,
                    ENABLE_TIME_ADJUSTER,
                    BINARY_HANDLING_MODE,
                    SCHEMA_NAME_ADJUSTMENT_MODE,
                    ROW_COUNT_FOR_STREAMING_RESULT_SETS,
                    INCREMENTAL_SNAPSHOT_CHUNK_SIZE,
                    INCREMENTAL_SNAPSHOT_ALLOW_SCHEMA_CHANGES,
                    STORE_ONLY_CAPTURED_DATABASES_DDL,
                    CONNECTOR_ADAPTER)
            .events(
                    INCLUDE_SQL_QUERY,
                    TABLE_IGNORE_BUILTIN,
                    DATABASE_INCLUDE_LIST,
                    DATABASE_EXCLUDE_LIST,
                    GTID_SOURCE_INCLUDES,
                    GTID_SOURCE_EXCLUDES,
                    GTID_SOURCE_FILTER_DML_EVENTS,
                    BUFFER_SIZE_FOR_BINLOG_READER,
                    EVENT_DESERIALIZATION_FAILURE_HANDLING_MODE,
                    INCONSISTENT_SCHEMA_HANDLING_MODE,
                    SOURCE_INFO_STRUCT_MAKER)
            .create();

    protected static ConfigDef configDef() {
        return CONFIG_DEFINITION.configDef();
    }

    /**
     * The set of {@link Field}s defined as part of this configuration.
     */
    public static Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    protected static final Set<String> BUILT_IN_DB_NAMES = Collect.unmodifiableSet("mysql", "performance_schema", "sys", "information_schema");

    @Override
    public boolean supportsOperationFiltering() {
        return true;
    }

    @Override
    protected boolean supportsSchemaChangesDuringIncrementalSnapshot() {
        return true;
    }

    private final Configuration config;
    private final SnapshotMode snapshotMode;
    private final SnapshotLockingMode snapshotLockingMode;
    private final SnapshotNewTables snapshotNewTables;
    private final TemporalPrecisionMode temporalPrecisionMode;
    private final Duration connectionTimeout;
    private final Predicate<String> gtidSourceFilter;
    private final EventProcessingFailureHandlingMode inconsistentSchemaFailureHandlingMode;
    private final boolean readOnlyConnection;
    private final ConnectorAdapter connectorAdapter;

    public MySqlConnectorConfig(Configuration config) {
        super(
                MySqlConnector.class,
                config,
                TableFilter.fromPredicate(MySqlConnectorConfig::isNotBuiltInTable),
                true,
                resolveDefaultFetchSize(config),
                ColumnFilterMode.CATALOG,
                false);

        this.config = config;
        this.temporalPrecisionMode = TemporalPrecisionMode.parse(config.getString(TIME_PRECISION_MODE));
        this.snapshotMode = SnapshotMode.parse(config.getString(SNAPSHOT_MODE), SNAPSHOT_MODE.defaultValueAsString());
        this.snapshotLockingMode = SnapshotLockingMode.parse(config.getString(SNAPSHOT_LOCKING_MODE), SNAPSHOT_LOCKING_MODE.defaultValueAsString());
        this.readOnlyConnection = config.getBoolean(READ_ONLY_CONNECTION);

        final String snapshotNewTables = config.getString(MySqlConnectorConfig.SNAPSHOT_NEW_TABLES);
        this.snapshotNewTables = SnapshotNewTables.parse(snapshotNewTables, MySqlConnectorConfig.SNAPSHOT_NEW_TABLES.defaultValueAsString());

        final String inconsistentSchemaFailureHandlingMode = config.getString(MySqlConnectorConfig.INCONSISTENT_SCHEMA_HANDLING_MODE);
        this.inconsistentSchemaFailureHandlingMode = EventProcessingFailureHandlingMode.parse(inconsistentSchemaFailureHandlingMode);

        this.connectionTimeout = Duration.ofMillis(config.getLong(MySqlConnectorConfig.CONNECTION_TIMEOUT_MS));

        // Set up the GTID filter ...
        final String gtidSetIncludes = config.getString(MySqlConnectorConfig.GTID_SOURCE_INCLUDES);
        final String gtidSetExcludes = config.getString(MySqlConnectorConfig.GTID_SOURCE_EXCLUDES);
        this.gtidSourceFilter = gtidSetIncludes != null ? Predicates.includesUuids(gtidSetIncludes)
                : (gtidSetExcludes != null ? Predicates.excludesUuids(gtidSetExcludes) : null);

        this.storeOnlyCapturedDatabasesDdl = config.getBoolean(STORE_ONLY_CAPTURED_DATABASES_DDL);

        // This should always be last to guarantee the full configuration is passed in the constructor
        this.connectorAdapter = ConnectorAdapterMode.parse(config.getString(CONNECTOR_ADAPTER)).getAdapter(this);
    }

    public boolean useCursorFetch() {
        return this.getSnapshotFetchSize() > 0;
    }

    public Optional<SnapshotLockingMode> getSnapshotLockingMode() {
        return Optional.of(this.snapshotLockingMode);
    }

    private static int validateEventDeserializationFailureHandlingModeNotSet(Configuration config, Field field, ValidationOutput problems) {
        final String modeName = config.asMap().get(EVENT_DESERIALIZATION_FAILURE_HANDLING_MODE.name());
        if (modeName != null) {
            LOGGER.warn("Configuration option '{}' is renamed to '{}'", EVENT_DESERIALIZATION_FAILURE_HANDLING_MODE.name(),
                    EVENT_PROCESSING_FAILURE_HANDLING_MODE.name());
        }
        return 0;
    }

    private static int validateGtidSetExcludes(Configuration config, Field field, ValidationOutput problems) {
        String includes = config.getString(GTID_SOURCE_INCLUDES);
        String excludes = config.getString(GTID_SOURCE_EXCLUDES);
        if (includes != null && excludes != null) {
            problems.accept(GTID_SOURCE_EXCLUDES, excludes, "Included GTID source UUIDs are already specified");
            return 1;
        }
        return 0;
    }

    /**
     * Validate the new snapshot.locking.mode configuration, which replaces snapshot.minimal.locking.
     *
     * If minimal.locking is explicitly defined and locking.mode is NOT explicitly defined:
     *   - coerce minimal.locking into the new snap.locking.mode property.
     *
     * If minimal.locking is NOT explicitly defined and locking.mode IS explicitly defined:
     *   - use new locking.mode property.
     *
     * If BOTH minimal.locking and locking.mode ARE defined:
     *   - Throw a validation error.
     */
    private static int validateSnapshotLockingMode(Configuration config, Field field, ValidationOutput problems) {
        // Determine which configurations are explicitly defined
        if (config.hasKey(SNAPSHOT_LOCKING_MODE.name())) {
            final SnapshotLockingMode lockingModeValue = SnapshotLockingMode.parse(
                    config.getString(MySqlConnectorConfig.SNAPSHOT_LOCKING_MODE));
            // Sanity check, validate the configured value is a valid option.
            if (lockingModeValue == null) {
                problems.accept(SNAPSHOT_LOCKING_MODE, lockingModeValue, "Must be a valid snapshot.locking.mode value");
                return 1;
            }
        }

        // Everything checks out ok.
        return 0;
    }

    /**
     * Validate the time.precision.mode configuration.
     *
     * If {@code adaptive} is specified, this option has the potential to cause overflow which is why the
     * option was deprecated and no longer supported for this connector.
     */
    private static int validateTimePrecisionMode(Configuration config, Field field, ValidationOutput problems) {
        if (config.hasKey(TIME_PRECISION_MODE.name())) {
            final String timePrecisionMode = config.getString(TIME_PRECISION_MODE.name());
            if (TemporalPrecisionMode.ADAPTIVE.getValue().equals(timePrecisionMode)) {
                // this is a problem
                problems.accept(TIME_PRECISION_MODE, timePrecisionMode, "The 'adaptive' time.precision.mode is no longer supported");
                return 1;
            }
        }

        // Everything checks out ok.
        return 0;
    }

    private static int resolveDefaultFetchSize(Configuration config) {
        if (usesMariaDbProtocol(config)) {
            // In order to mimic MySQL's Integer.MIN_VALUE logic which indicates to stream 1 row
            // at a time, for MariaDB we need to explicitly set the driver with a fetch size of
            // 1 as MariaDB drivers 3.x+ do not support the old non-compliant JDBC-spec style
            // that MySQL uses.
            return 1;
        }
        return DEFAULT_SNAPSHOT_FETCH_SIZE;
    }

    @Override
    protected SourceInfoStructMaker<? extends AbstractSourceInfo> getSourceInfoStructMaker(Version version) {
        return getSourceInfoStructMaker(SOURCE_INFO_STRUCT_MAKER, Module.name(), Module.version(), this);
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
    public TemporalPrecisionMode getTemporalPrecisionMode() {
        return temporalPrecisionMode;
    }

    public EnumeratedValue getSnapshotMode() {
        return snapshotMode;
    }

    public Duration getConnectionTimeout() {
        return connectionTimeout;
    }

    public EventProcessingFailureHandlingMode inconsistentSchemaFailureHandlingMode() {
        return inconsistentSchemaFailureHandlingMode;
    }

    public String hostname() {
        return config.getString(HOSTNAME);
    }

    public int port() {
        return config.getInteger(PORT);
    }

    public String username() {
        return config.getString(USER);
    }

    public String password() {
        return config.getString(PASSWORD);
    }

    public long serverId() {
        return config.getLong(SERVER_ID);
    }

    public SecureConnectionMode sslMode() {
        final String mode = config.getString(MySqlConnectorConfig.SSL_MODE);
        return SecureConnectionMode.parse(mode);
    }

    public boolean sslModeEnabled() {
        return sslMode() != SecureConnectionMode.DISABLED;
    }

    public int bufferSizeForStreamingChangeEventSource() {
        return config.getInteger(MySqlConnectorConfig.BUFFER_SIZE_FOR_BINLOG_READER);
    }

    public ConnectorAdapter getConnectorAdapter() {
        return connectorAdapter;
    }

    /**
     * Get the predicate function that will return {@code true} if a GTID source is to be included, or {@code false} if
     * a GTID source is to be excluded.
     *
     * @return the GTID source predicate function; never null
     */
    public Predicate<String> gtidSourceFilter() {
        return gtidSourceFilter;
    }

    public boolean includeSchemaChangeRecords() {
        return config.getBoolean(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES);
    }

    public boolean includeSqlQuery() {
        return config.getBoolean(MySqlConnectorConfig.INCLUDE_SQL_QUERY);
    }

    public long rowCountForLargeTable() {
        return config.getLong(MySqlConnectorConfig.ROW_COUNT_FOR_STREAMING_RESULT_SETS);
    }

    @Override
    protected HistoryRecordComparator getHistoryRecordComparator() {
        return connectorAdapter.getHistoryRecordComparator();
    }

    public static boolean isBuiltInDatabase(String databaseName) {
        if (databaseName == null) {
            return false;
        }
        return BUILT_IN_DB_NAMES.contains(databaseName.toLowerCase());
    }

    public static boolean isNotBuiltInTable(TableId id) {
        return !isBuiltInDatabase(id.catalog());
    }

    public boolean isReadOnlyConnection() {
        return readOnlyConnection;
    }

    /**
     * Intended for testing only
     */
    boolean useGlobalLock() {
        return !"true".equals(config.getString(TEST_DISABLE_GLOBAL_LOCKING));
    }

    public boolean usesMariaDbProtocol() {
        return usesMariaDbProtocol(config);
    }

    public static boolean usesMariaDbProtocol(Configuration config) {
        if (config.hasKey(MySqlConnectorConfig.JDBC_PROTOCOL)) {
            final String protocol = config.getString(MySqlConnectorConfig.JDBC_PROTOCOL);
            if (!Strings.isNullOrBlank(protocol) && protocol.equalsIgnoreCase("jdbc:mariadb")) {
                return true;
            }
        }
        return false;
    }
}
