/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Random;

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
import io.debezium.config.Field.ValidationOutput;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.jdbc.JdbcValueConverters.BigIntUnsignedMode;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.history.DatabaseHistory;
import io.debezium.relational.history.KafkaDatabaseHistory;

/**
 * The configuration properties.
 */
public class MySqlConnectorConfig extends RelationalDatabaseConnectorConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlConnectorConfig.class);

    /**
     * The set of predefined BigIntUnsignedHandlingMode options or aliases.
     */
    public static enum BigIntUnsignedHandlingMode implements EnumeratedValue {
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

        private BigIntUnsignedHandlingMode(String value) {
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
    public static enum SnapshotMode implements EnumeratedValue {

        /**
         * Perform a snapshot when it is needed.
         */
        WHEN_NEEDED("when_needed", true),

        /**
         * Perform a snapshot only upon initial startup of a connector.
         */
        INITIAL("initial", true),

        /**
         * Perform a snapshot of only the database schemas (without data) and then begin reading the binlog.
         * This should be used with care, but it is very useful when the change event consumers need only the changes
         * from the point in time the snapshot is made (and doesn't care about any state or changes prior to this point).
         */
        SCHEMA_ONLY("schema_only", false),

        /**
         * Perform a snapshot of only the database schemas (without data) and then begin reading the binlog at the current binlog position.
         * This can be used for recovery only if the connector has existing offsets and the database.history.kafka.topic does not exist (deleted).
         * This recovery option should be used with care as it assumes there have been no schema changes since the connector last stopped,
         * otherwise some events during the gap may be processed with an incorrect schema and corrupted.
         */
        SCHEMA_ONLY_RECOVERY("schema_only_recovery", false),

        /**
         * Never perform a snapshot and only read the binlog. This assumes the binlog contains all the history of those
         * databases and tables that will be captured.
         */
        NEVER("never", false),

        /**
         * Perform a snapshot and then stop before attempting to read the binlog.
         */
        INITIAL_ONLY("initial_only", true);

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

    public static enum SnapshotNewTables implements EnumeratedValue {
        /**
         * Do not snapshot new tables
         */
        OFF("off"),

        /**
         * Snapshot new tables in parallel to normal binlog reading.
         */
        PARALLEL("parallel");

        private final String value;

        private SnapshotNewTables(String value) {
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
    public static enum SnapshotLockingMode implements EnumeratedValue {

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
    public static enum SecureConnectionMode implements EnumeratedValue {
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

        private SecureConnectionMode(String value) {
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
     * The set of predefined Gtid New Channel Position options.
     */
    public static enum GtidNewChannelPosition implements EnumeratedValue {

        /**
         * This mode will start reading new gtid channel from mysql servers last_executed position
         */
        LATEST("latest"),

        /**
         * This mode will start reading new gtid channel from earliest available position in server.
         * This is needed when during active-passive failover the new gtid channel becomes active and receiving writes. #DBZ-923
         */
        EARLIEST("earliest");

        private final String value;

        private GtidNewChannelPosition(String value) {
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
        public static GtidNewChannelPosition parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (GtidNewChannelPosition option : GtidNewChannelPosition.values()) {
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
        public static GtidNewChannelPosition parse(String value, String defaultValue) {
            GtidNewChannelPosition mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    /**
     * {@link Integer#MIN_VALUE Minimum value} used for fetch size hint.
     * See <a href="https://issues.jboss.org/browse/DBZ-94">DBZ-94</a> for details.
     */
    protected static final int DEFAULT_SNAPSHOT_FETCH_SIZE = Integer.MIN_VALUE;

    private static final String DATABASE_WHITELIST_NAME = "database.whitelist";
    private static final String DATABASE_BLACKLIST_NAME = "database.blacklist";

    /**
     * Default size of the binlog buffer used for examining transactions and
     * deciding whether to propagate them or not. A size of 0 disables the buffer,
     * all events will be passed on directly as they are passed by the binlog
     * client.
     */
    private static final int DEFAULT_BINLOG_BUFFER_SIZE = 0;

    public static final Field HOSTNAME = Field.create("database.hostname")
            .withDisplayName("Hostname")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withValidation(Field::isRequired)
            .withDescription("Resolvable hostname or IP address of the MySQL database server.");

    public static final Field PORT = Field.create("database.port")
            .withDisplayName("Port")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withDefault(3306)
            .withImportance(Importance.HIGH)
            .withValidation(Field::isInteger)
            .withDescription("Port of the MySQL database server.");

    public static final Field USER = Field.create("database.user")
            .withDisplayName("User")
            .withType(Type.STRING)
            .withWidth(Width.SHORT)
            .withImportance(Importance.HIGH)
            .withValidation(Field::isRequired)
            .withDescription("Name of the MySQL database user to be used when connecting to the database.");

    public static final Field PASSWORD = Field.create("database.password")
            .withDisplayName("Password")
            .withType(Type.PASSWORD)
            .withWidth(Width.SHORT)
            .withImportance(Importance.HIGH)
            .withDescription("Password of the MySQL database user to be used when connecting to the database.");

    public static final Field ON_CONNECT_STATEMENTS = Field.create("database.initial.statements")
            .withDisplayName("Initial statements")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withDescription(
                    "A semicolon separated list of SQL statements to be executed when a JDBC connection (not binlog reading connection) to the database is established. "
                            + "Note that the connector may establish JDBC connections at its own discretion, so this should typically be used for configuration of session parameters only,"
                            + "but not for executing DML statements. Use doubled semicolon (';;') to use a semicolon as a character and not as a delimiter.");

    public static final Field SERVER_NAME = RelationalDatabaseConnectorConfig.SERVER_NAME
            .withValidation(CommonConnectorConfig::validateServerNameIsDifferentFromHistoryTopicName);

    public static final Field SERVER_ID = Field.create("database.server.id")
            .withDisplayName("Cluster ID")
            .withType(Type.LONG)
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withDefault(MySqlConnectorConfig::randomServerId)
            .withValidation(Field::isRequired, Field::isPositiveLong)
            .withDescription("A numeric ID of this database client, which must be unique across all "
                    + "currently-running database processes in the cluster. This connector joins the "
                    + "MySQL database cluster as another server (with this unique ID) so it can read "
                    + "the binlog. By default, a random number is generated between 5400 and 6400.");

    public static final Field SERVER_ID_OFFSET = Field.create("database.server.id.offset")
            .withDisplayName("Cluster ID offset")
            .withType(Type.LONG)
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withDefault(10000L)
            .withDescription("Only relevant if parallel snapshotting is configured. During "
                    + "parallel snapshotting, multiple (4) connections open to the database "
                    + "client, and they each need their own unique connection ID. This offset is "
                    + "used to generate those IDs from the base configured cluster ID.");

    public static final Field SSL_MODE = Field.create("database.ssl.mode")
            .withDisplayName("SSL mode")
            .withEnum(SecureConnectionMode.class, SecureConnectionMode.DISABLED)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDescription("Whether to use an encrypted connection to MySQL. Options include"
                    + "'disabled' (the default) to use an unencrypted connection; "
                    + "'preferred' to establish a secure (encrypted) connection if the server supports secure connections, "
                    + "but fall back to an unencrypted connection otherwise; "
                    + "'required' to use a secure (encrypted) connection, and fail if one cannot be established; "
                    + "'verify_ca' like 'required' but additionally verify the server TLS certificate against the configured Certificate Authority "
                    + "(CA) certificates, or fail if no valid matching CA certificates are found; or"
                    + "'verify_identity' like 'verify_ca' but additionally verify that the server certificate matches the host to which the connection is attempted.");

    public static final Field SSL_KEYSTORE = Field.create("database.ssl.keystore")
            .withDisplayName("SSL Keystore")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDescription("Location of the Java keystore file containing an application process's own certificate and private key.");

    public static final Field SSL_KEYSTORE_PASSWORD = Field.create("database.ssl.keystore.password")
            .withDisplayName("SSL Keystore Password")
            .withType(Type.PASSWORD)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDescription(
                    "Password to access the private key from the keystore file specified by 'ssl.keystore' configuration property or the 'javax.net.ssl.keyStore' system or JVM property. "
                            + "This password is used to unlock the keystore file (store password), and to decrypt the private key stored in the keystore (key password).");

    public static final Field SSL_TRUSTSTORE = Field.create("database.ssl.truststore")
            .withDisplayName("SSL Truststore")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDescription("Location of the Java truststore file containing the collection of CA certificates trusted by this application process (trust store).");

    public static final Field SSL_TRUSTSTORE_PASSWORD = Field.create("database.ssl.truststore.password")
            .withDisplayName("SSL Truststore Password")
            .withType(Type.PASSWORD)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDescription(
                    "Password to unlock the keystore file (store password) specified by 'ssl.trustore' configuration property or the 'javax.net.ssl.trustStore' system or JVM property.");

    public static final Field TABLES_IGNORE_BUILTIN = RelationalDatabaseConnectorConfig.TABLE_IGNORE_BUILTIN
            .withDependents(DATABASE_WHITELIST_NAME);

    public static final Field JDBC_DRIVER = Field.create("database.jdbc.driver")
            .withDisplayName("Jdbc Driver Class Name")
            .withType(Type.CLASS)
            .withWidth(Width.MEDIUM)
            .withDefault(com.mysql.cj.jdbc.Driver.class.getName())
            .withImportance(Importance.LOW)
            .withValidation(Field::isClassName)
            .withDescription("JDBC Driver class name used to connect to the MySQL database server.");
    /**
     * A comma-separated list of regular expressions that match database names to be monitored.
     * May not be used with {@link #DATABASE_BLACKLIST}.
     */
    public static final Field DATABASE_WHITELIST = Field.create(DATABASE_WHITELIST_NAME)
            .withDisplayName("Databases")
            .withType(Type.LIST)
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withDependents(TABLE_WHITELIST_NAME)
            .withDescription("The databases for which changes are to be captured");

    /**
     * A comma-separated list of regular expressions that match database names to be excluded from monitoring.
     * May not be used with {@link #DATABASE_WHITELIST}.
     */
    public static final Field DATABASE_BLACKLIST = Field.create(DATABASE_BLACKLIST_NAME)
            .withDisplayName("Exclude Databases")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withValidation(MySqlConnectorConfig::validateDatabaseBlacklist)
            .withInvisibleRecommender()
            .withDescription("");

    /**
     * A comma-separated list of regular expressions that match source UUIDs in the GTID set used to find the binlog
     * position in the MySQL server. Only the GTID ranges that have sources matching one of these include patterns will
     * be used.
     * May not be used with {@link #GTID_SOURCE_EXCLUDES}.
     */
    public static final Field GTID_SOURCE_INCLUDES = Field.create("gtid.source.includes")
            .withDisplayName("Include GTID sources")
            .withType(Type.LIST)
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withDependents(TABLE_WHITELIST_NAME)
            .withDescription("The source UUIDs used to include GTID ranges when determine the starting position in the MySQL server's binlog.");

    /**
     * A comma-separated list of regular expressions that match source UUIDs in the GTID set used to find the binlog
     * position in the MySQL server. Only the GTID ranges that have sources matching none of these exclude patterns will
     * be used.
     * May not be used with {@link #GTID_SOURCE_INCLUDES}.
     */
    public static final Field GTID_SOURCE_EXCLUDES = Field.create("gtid.source.excludes")
            .withDisplayName("Exclude GTID sources")
            .withType(Type.STRING)
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
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(true)
            .withDescription(
                    "If set to true, we will only produce DML events into Kafka for transactions that were written on mysql servers with UUIDs matching the filters defined by the gtid.source.includes or gtid.source.excludes configuration options, if they are specified.");

    /**
     * If set to 'latest', connector when encountering new GTID channel after job restart will start reading it from the
     * latest executed position (default). When set to 'earliest' the connector will start reading new GTID channels from the first available position.
     * This is useful when in active-passive mysql setup during failover new GTID channel starts receiving writes, see DBZ-923.
     *
     * Defaults to latest.
     */
    public static final Field GTID_NEW_CHANNEL_POSITION = Field.create("gtid.new.channel.position")
            .withDisplayName("GTID start position")
            .withEnum(GtidNewChannelPosition.class, GtidNewChannelPosition.EARLIEST)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withValidation(MySqlConnectorConfig::validateGtidNewChannelPositionNotSet)
            .withDescription(
                    "If set to 'latest', when connector sees new GTID, it will start consuming gtid channel from the server latest executed gtid position. If 'earliest' (the default) connector starts reading channel from first available (not purged) gtid position on the server.");

    public static final Field CONNECTION_TIMEOUT_MS = Field.create("connect.timeout.ms")
            .withDisplayName("Connection Timeout (ms)")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Maximum time in milliseconds to wait after trying to connect to the database before timing out.")
            .withDefault(30 * 1000)
            .withValidation(Field::isPositiveInteger);

    public static final Field KEEP_ALIVE = Field.create("connect.keep.alive")
            .withDisplayName("Keep connection alive (true/false)")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Whether a separate thread should be used to ensure the connection is kept alive.")
            .withDefault(true)
            .withValidation(Field::isBoolean);

    public static final Field KEEP_ALIVE_INTERVAL_MS = Field.create("connect.keep.alive.interval.ms")
            .withDisplayName("Keep alive interval (ms)")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Interval in milliseconds to wait for connection checking if keep alive thread is used.")
            .withDefault(Duration.ofMinutes(1).toMillis())
            .withValidation(Field::isPositiveInteger);

    public static final Field ROW_COUNT_FOR_STREAMING_RESULT_SETS = Field.create("min.row.count.to.stream.results")
            .withDisplayName("Stream result set of size")
            .withType(Type.LONG)
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
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDescription("The size of a look-ahead buffer used by the  binlog reader to decide whether "
                    + "the transaction in progress is going to be committed or rolled back. "
                    + "Use 0 to disable look-ahead buffering. "
                    + "Defaults to " + DEFAULT_BINLOG_BUFFER_SIZE + " (i.e. buffering is disabled).")
            .withDefault(DEFAULT_BINLOG_BUFFER_SIZE)
            .withValidation(Field::isNonNegativeInteger);

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

    public static final Field INCLUDE_SQL_QUERY = Field.create("include.query")
            .withDisplayName("Include original SQL query with in change events")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Whether the connector should include the original SQL query that generated the change event. "
                    + "Note: This option requires MySQL be configured with the binlog_rows_query_log_events option set to ON. Query will not be present for events generated from snapshot. "
                    + "WARNING: Enabling this option may expose tables or fields explicitly blacklisted or masked by including the original SQL statement in the change event. "
                    + "For this reason the default value is 'false'.")
            .withDefault(false);

    public static final Field SNAPSHOT_MODE = Field.create("snapshot.mode")
            .withDisplayName("Snapshot mode")
            .withEnum(SnapshotMode.class, SnapshotMode.INITIAL)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("The criteria for running a snapshot upon startup of the connector. "
                    + "Options include: "
                    + "'when_needed' to specify that the connector run a snapshot upon startup whenever it deems it necessary; "
                    + "'schema_only' to only take a snapshot of the schema (table structures) but no actual data; "
                    + "'initial' (the default) to specify the connector can run a snapshot only when no offsets are available for the logical server name; "
                    + "'initial_only' same as 'initial' except the connector should stop after completing the snapshot and before it would normally read the binlog; and"
                    + "'never' to specify the connector should never run a snapshot and that upon first startup the connector should read from the beginning of the binlog. "
                    + "The 'never' mode should be used with care, and only when the binlog is known to contain all history.");

    public static final Field SNAPSHOT_LOCKING_MODE = Field.create("snapshot.locking.mode")
            .withDisplayName("Snapshot locking mode")
            .withEnum(SnapshotLockingMode.class, SnapshotLockingMode.MINIMAL)
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
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("BETA FEATURE: On connector restart, the connector will check if there have been any new tables added to the configuration, "
                    + "and snapshot them. There is presently only two options:"
                    + "'off': Default behavior. Do not snapshot new tables."
                    + "'parallel': The snapshot of the new tables will occur in parallel to the continued binlog reading of the old tables. When the snapshot "
                    + "completes, an independent binlog reader will begin reading the events for the new tables until it catches up to present time. At this "
                    + "point, both old and new binlog readers will be momentarily halted and new binlog reader will start that will read the binlog for all "
                    + "configured tables. The parallel binlog reader will have a configured server id of 10000 + the primary binlog reader's server id.");

    public static final Field TIME_PRECISION_MODE = RelationalDatabaseConnectorConfig.TIME_PRECISION_MODE
            .withEnum(TemporalPrecisionMode.class, TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS)
            .withValidation(MySqlConnectorConfig::validateTimePrecisionMode)
            .withDescription("Time, date and timestamps can be represented with different kinds of precisions, including:"
                    + "'adaptive_time_microseconds': the precision of date and timestamp values is based the database column's precision; but time fields always use microseconds precision;"
                    + "'connect': always represents time, date and timestamp values using Kafka Connect's built-in representations for Time, Date, and Timestamp, "
                    + "which uses millisecond precision regardless of the database columns' precision.");

    public static final Field BIGINT_UNSIGNED_HANDLING_MODE = Field.create("bigint.unsigned.handling.mode")
            .withDisplayName("BIGINT UNSIGNED Handling")
            .withEnum(BigIntUnsignedHandlingMode.class, BigIntUnsignedHandlingMode.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Specify how BIGINT UNSIGNED columns should be represented in change events, including:"
                    + "'precise' uses java.math.BigDecimal to represent values, which are encoded in the change events using a binary representation and Kafka Connect's 'org.apache.kafka.connect.data.Decimal' type; "
                    + "'long' (the default) represents values using Java's 'long', which may not offer the precision but will be far easier to use in consumers.");

    public static final Field EVENT_DESERIALIZATION_FAILURE_HANDLING_MODE = Field.create("event.deserialization.failure.handling.mode")
            .withDisplayName("Event deserialization failure handling")
            .withEnum(EventProcessingFailureHandlingMode.class, EventProcessingFailureHandlingMode.FAIL)
            .withValidation(MySqlConnectorConfig::validateEventDeserializationFailureHandlingModeNotSet)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Specify how failures during deserialization of binlog events (i.e. when encountering a corrupted event) should be handled, including:"
                    + "'fail' (the default) an exception indicating the problematic event and its binlog position is raised, causing the connector to be stopped; "
                    + "'warn' the problematic event and its binlog position will be logged and the event will be skipped;"
                    + "'ignore' the problematic event will be skipped.");

    public static final Field INCONSISTENT_SCHEMA_HANDLING_MODE = Field.create("inconsistent.schema.handling.mode")
            .withDisplayName("Inconsistent schema failure handling")
            .withEnum(EventProcessingFailureHandlingMode.class, EventProcessingFailureHandlingMode.FAIL)
            .withValidation(MySqlConnectorConfig::validateInconsistentSchemaHandlingModeNotIgnore)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription(
                    "Specify how binlog events that belong to a table missing from internal schema representation (i.e. internal representation is not consistent with database) should be handled, including:"
                            + "'fail' (the default) an exception indicating the problematic event and its binlog position is raised, causing the connector to be stopped; "
                            + "'warn' the problematic event and its binlog position will be logged and the event will be skipped;"
                            + "'skip' the problematic event will be skipped.");

    public static final Field ENABLE_TIME_ADJUSTER = Field.create("enable.time.adjuster")
            .withDisplayName("Enable Time Adjuster")
            .withType(Type.BOOLEAN)
            .withDefault(true)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription(
                    "MySQL allows user to insert year value as either 2-digit or 4-digit. In case of two digit the value is automatically mapped into 1970 - 2069." +
                            "false - delegates the implicit conversion to the database" +
                            "true - (the default) Debezium makes the conversion");

    /**
     * The set of {@link Field}s defined as part of this configuration.
     */
    public static Field.Set ALL_FIELDS = Field.setOf(USER, PASSWORD, HOSTNAME, PORT, ON_CONNECT_STATEMENTS, SERVER_ID, SERVER_ID_OFFSET,
            SERVER_NAME,
            CONNECTION_TIMEOUT_MS, KEEP_ALIVE, KEEP_ALIVE_INTERVAL_MS,
            CommonConnectorConfig.MAX_QUEUE_SIZE,
            CommonConnectorConfig.MAX_BATCH_SIZE,
            CommonConnectorConfig.POLL_INTERVAL_MS,
            BUFFER_SIZE_FOR_BINLOG_READER, Heartbeat.HEARTBEAT_INTERVAL,
            Heartbeat.HEARTBEAT_TOPICS_PREFIX, DATABASE_HISTORY, INCLUDE_SCHEMA_CHANGES, INCLUDE_SQL_QUERY,
            TABLE_WHITELIST, TABLE_BLACKLIST, TABLES_IGNORE_BUILTIN,
            DATABASE_WHITELIST, DATABASE_BLACKLIST,
            COLUMN_BLACKLIST, MSG_KEY_COLUMNS,
            RelationalDatabaseConnectorConfig.MASK_COLUMN_WITH_HASH,
            RelationalDatabaseConnectorConfig.MASK_COLUMN,
            RelationalDatabaseConnectorConfig.TRUNCATE_COLUMN,
            SNAPSHOT_MODE, SNAPSHOT_NEW_TABLES, SNAPSHOT_LOCKING_MODE,
            RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE,
            GTID_SOURCE_INCLUDES, GTID_SOURCE_EXCLUDES,
            GTID_SOURCE_FILTER_DML_EVENTS,
            GTID_NEW_CHANNEL_POSITION,
            TIME_PRECISION_MODE, DECIMAL_HANDLING_MODE,
            SSL_MODE, SSL_KEYSTORE, SSL_KEYSTORE_PASSWORD,
            SSL_TRUSTSTORE, SSL_TRUSTSTORE_PASSWORD, JDBC_DRIVER,
            BIGINT_UNSIGNED_HANDLING_MODE,
            EVENT_DESERIALIZATION_FAILURE_HANDLING_MODE,
            CommonConnectorConfig.EVENT_PROCESSING_FAILURE_HANDLING_MODE,
            INCONSISTENT_SCHEMA_HANDLING_MODE,
            CommonConnectorConfig.SNAPSHOT_DELAY_MS,
            CommonConnectorConfig.SNAPSHOT_FETCH_SIZE,
            CommonConnectorConfig.TOMBSTONES_ON_DELETE, ENABLE_TIME_ADJUSTER,
            CommonConnectorConfig.SOURCE_STRUCT_MAKER_VERSION,
            CommonConnectorConfig.SKIPPED_OPERATIONS,
            BINARY_HANDLING_MODE);

    /**
     * The set of {@link Field}s that are included in the {@link #configDef() configuration definition}. This includes
     * all fields defined in this class (though some are always invisible since they are not to be exposed to the user interface)
     * plus several that are specific to the {@link KafkaDatabaseHistory} class, since history is always stored in Kafka
     * when run via the user interface.
     */
    protected static Field.Set EXPOSED_FIELDS = ALL_FIELDS.with(KafkaDatabaseHistory.BOOTSTRAP_SERVERS,
            KafkaDatabaseHistory.TOPIC,
            KafkaDatabaseHistory.RECOVERY_POLL_ATTEMPTS,
            KafkaDatabaseHistory.RECOVERY_POLL_INTERVAL_MS,
            KafkaDatabaseHistory.INTERNAL_CONNECTOR_CLASS,
            KafkaDatabaseHistory.INTERNAL_CONNECTOR_ID,
            DatabaseHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS,
            DatabaseHistory.STORE_ONLY_MONITORED_TABLES_DDL,
            DatabaseHistory.DDL_FILTER);

    private final SnapshotLockingMode snapshotLockingMode;
    private final GtidNewChannelPosition gitIdNewChannelPosition;
    private final SnapshotNewTables snapshotNewTables;
    private final TemporalPrecisionMode temporalPrecisionMode;
    private final Duration connectionTimeout;

    public MySqlConnectorConfig(Configuration config) {
        super(
                config,
                config.getString(SERVER_NAME),
                null, // TODO whitelist handling is still done locally here
                null,
                DEFAULT_SNAPSHOT_FETCH_SIZE);

        this.temporalPrecisionMode = TemporalPrecisionMode.parse(config.getString(TIME_PRECISION_MODE));
        this.snapshotLockingMode = SnapshotLockingMode.parse(config.getString(SNAPSHOT_LOCKING_MODE), SNAPSHOT_LOCKING_MODE.defaultValueAsString());

        String gitIdNewChannelPosition = config.getString(MySqlConnectorConfig.GTID_NEW_CHANNEL_POSITION);
        this.gitIdNewChannelPosition = GtidNewChannelPosition.parse(gitIdNewChannelPosition, MySqlConnectorConfig.GTID_NEW_CHANNEL_POSITION.defaultValueAsString());

        String snapshotNewTables = config.getString(MySqlConnectorConfig.SNAPSHOT_NEW_TABLES);
        this.snapshotNewTables = SnapshotNewTables.parse(snapshotNewTables, MySqlConnectorConfig.SNAPSHOT_NEW_TABLES.defaultValueAsString());

        this.connectionTimeout = Duration.ofMillis(config.getLong(MySqlConnectorConfig.CONNECTION_TIMEOUT_MS));
    }

    public SnapshotLockingMode getSnapshotLockingMode() {
        return this.snapshotLockingMode;
    }

    public GtidNewChannelPosition gtidNewChannelPosition() {
        return gitIdNewChannelPosition;
    }

    public SnapshotNewTables getSnapshotNewTables() {
        return snapshotNewTables;
    }

    protected static ConfigDef configDef() {
        ConfigDef config = new ConfigDef();
        Field.group(config, "MySQL", HOSTNAME, PORT, USER, PASSWORD, ON_CONNECT_STATEMENTS, SERVER_NAME, SERVER_ID, SERVER_ID_OFFSET,
                SSL_MODE, SSL_KEYSTORE, SSL_KEYSTORE_PASSWORD, SSL_TRUSTSTORE, SSL_TRUSTSTORE_PASSWORD, JDBC_DRIVER, CommonConnectorConfig.SKIPPED_OPERATIONS);
        Field.group(config, "History Storage", KafkaDatabaseHistory.BOOTSTRAP_SERVERS,
                KafkaDatabaseHistory.TOPIC, KafkaDatabaseHistory.RECOVERY_POLL_ATTEMPTS,
                KafkaDatabaseHistory.RECOVERY_POLL_INTERVAL_MS, DATABASE_HISTORY,
                DatabaseHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS, DatabaseHistory.DDL_FILTER,
                DatabaseHistory.STORE_ONLY_MONITORED_TABLES_DDL);
        Field.group(config, "Events", INCLUDE_SCHEMA_CHANGES, INCLUDE_SQL_QUERY, TABLES_IGNORE_BUILTIN, DATABASE_WHITELIST, TABLE_WHITELIST,
                COLUMN_BLACKLIST, TABLE_BLACKLIST, DATABASE_BLACKLIST, MSG_KEY_COLUMNS,
                RelationalDatabaseConnectorConfig.MASK_COLUMN_WITH_HASH,
                RelationalDatabaseConnectorConfig.MASK_COLUMN,
                RelationalDatabaseConnectorConfig.TRUNCATE_COLUMN,
                RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE,
                GTID_SOURCE_INCLUDES, GTID_SOURCE_EXCLUDES, GTID_SOURCE_FILTER_DML_EVENTS, GTID_NEW_CHANNEL_POSITION, BUFFER_SIZE_FOR_BINLOG_READER,
                Heartbeat.HEARTBEAT_INTERVAL, Heartbeat.HEARTBEAT_TOPICS_PREFIX, EVENT_DESERIALIZATION_FAILURE_HANDLING_MODE,
                CommonConnectorConfig.EVENT_PROCESSING_FAILURE_HANDLING_MODE, INCONSISTENT_SCHEMA_HANDLING_MODE,
                CommonConnectorConfig.TOMBSTONES_ON_DELETE, CommonConnectorConfig.SOURCE_STRUCT_MAKER_VERSION);
        Field.group(config, "Connector", CONNECTION_TIMEOUT_MS, KEEP_ALIVE, KEEP_ALIVE_INTERVAL_MS, CommonConnectorConfig.MAX_QUEUE_SIZE,
                CommonConnectorConfig.MAX_BATCH_SIZE, CommonConnectorConfig.POLL_INTERVAL_MS,
                SNAPSHOT_MODE, SNAPSHOT_LOCKING_MODE, SNAPSHOT_NEW_TABLES, TIME_PRECISION_MODE, DECIMAL_HANDLING_MODE,
                BIGINT_UNSIGNED_HANDLING_MODE, SNAPSHOT_DELAY_MS, SNAPSHOT_FETCH_SIZE, ENABLE_TIME_ADJUSTER, BINARY_HANDLING_MODE);
        return config;
    }

    private static int validateGtidNewChannelPositionNotSet(Configuration config, Field field, ValidationOutput problems) {
        if (config.getString(GTID_NEW_CHANNEL_POSITION.name()) != null) {
            LOGGER.warn("Configuration option '{}' is deprecated and scheduled for removal", GTID_NEW_CHANNEL_POSITION.name());
        }
        return 0;
    }

    private static int validateEventDeserializationFailureHandlingModeNotSet(Configuration config, Field field, ValidationOutput problems) {
        final String modeName = config.asMap().get(EVENT_DESERIALIZATION_FAILURE_HANDLING_MODE.name());
        if (modeName != null) {
            LOGGER.warn("Configuration option '{}' is renamed to '{}'", EVENT_DESERIALIZATION_FAILURE_HANDLING_MODE.name(),
                    EVENT_PROCESSING_FAILURE_HANDLING_MODE.name());
            if (EventProcessingFailureHandlingMode.OBSOLETE_NAME_FOR_SKIP_FAILURE_HANDLING.equals(modeName)) {
                LOGGER.warn("Value '{}' of configuration option '{}' is deprecated and should be replaced with '{}'",
                        EventProcessingFailureHandlingMode.OBSOLETE_NAME_FOR_SKIP_FAILURE_HANDLING,
                        EVENT_DESERIALIZATION_FAILURE_HANDLING_MODE.name(),
                        EventProcessingFailureHandlingMode.SKIP.getValue());
            }
        }
        return 0;
    }

    private static int validateInconsistentSchemaHandlingModeNotIgnore(Configuration config, Field field, ValidationOutput problems) {
        final String modeName = config.getString(INCONSISTENT_SCHEMA_HANDLING_MODE);
        if (EventProcessingFailureHandlingMode.OBSOLETE_NAME_FOR_SKIP_FAILURE_HANDLING.equals(modeName)) {
            LOGGER.warn("Value '{}' of configuration option '{}' is deprecated and should be replaced with '{}'",
                    EventProcessingFailureHandlingMode.OBSOLETE_NAME_FOR_SKIP_FAILURE_HANDLING,
                    INCONSISTENT_SCHEMA_HANDLING_MODE.name(),
                    EventProcessingFailureHandlingMode.SKIP.getValue());
        }
        return 0;
    }

    private static int validateDatabaseBlacklist(Configuration config, Field field, ValidationOutput problems) {
        String whitelist = config.getString(DATABASE_WHITELIST);
        String blacklist = config.getString(DATABASE_BLACKLIST);
        if (whitelist != null && blacklist != null) {
            problems.accept(DATABASE_BLACKLIST, blacklist, "Whitelist is already specified");
            return 1;
        }
        return 0;
    }

    private static int validateTableBlacklist(Configuration config, Field field, ValidationOutput problems) {
        String whitelist = config.getString(TABLE_WHITELIST);
        String blacklist = config.getString(TABLE_BLACKLIST);
        if (whitelist != null && blacklist != null) {
            problems.accept(TABLE_BLACKLIST, blacklist, "Whitelist is already specified");
            return 1;
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

    private static int randomServerId() {
        int lowestServerId = 5400;
        int highestServerId = 6400;
        return lowestServerId + new Random().nextInt(highestServerId - lowestServerId);
    }

    @Override
    protected SourceInfoStructMaker<? extends AbstractSourceInfo> getSourceInfoStructMaker(Version version) {
        switch (version) {
            case V1:
                return new LegacyV1MySqlSourceInfoStructMaker(Module.name(), Module.version(), this);
            default:
                return new MySqlSourceInfoStructMaker(Module.name(), Module.version(), this);
        }
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

    public Duration getConnectionTimeout() {
        return connectionTimeout;
    }
}
