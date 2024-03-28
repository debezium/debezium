/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import java.time.Duration;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.config.Field.ValidationOutput;
import io.debezium.connector.binlog.gtid.GtidSet;
import io.debezium.connector.binlog.gtid.GtidSetFactory;
import io.debezium.jdbc.JdbcValueConverters.BigIntUnsignedMode;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.schema.DefaultTopicNamingStrategy;
import io.debezium.util.Collect;

/**
 * Configuration properties for binlog-based connectors.
 *
 * @author Chris Cranford
 */
public abstract class BinlogConnectorConfig extends HistorizedRelationalDatabaseConnectorConfig {

    // todo: reassign field groups

    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogConnectorConfig.class);

    /**
     * It is not possible to test disabled global locking locally as binlog-based connectors builds
     * always provide global locking. So to bypass this limitation, it is necessary to provide a
     * backdoor to the connector to disable it on its own.
     */
    static final String TEST_DISABLE_GLOBAL_LOCKING = "test.disable.global.locking";

    /**
     * Set of predefined BigIntUnsignedHandlingMode options or aliases.
     */
    public enum BigIntUnsignedHandlingMode implements EnumeratedValue {
        /**
         * Represents {@code BIGINT UNSIGNED} values as precise {@link java.math.BigDecimal} values, which
         * are represented in change events in binary form. This is precise but difficult to use.
         */
        PRECISE("precise"),
        /**
         * Represents {@code BIGINT UNSIGNED} values as precise {@code long} values. This may be less
         * precise but is far easier to use.
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

        public BigIntUnsignedMode asBigIntUnsignedMode() {
            switch (this) {
                case LONG:
                    return BigIntUnsignedMode.LONG;
                case PRECISE:
                default:
                    return BigIntUnsignedMode.PRECISE;
            }
        }
    }

    /**
     * Set of predefined SnapshotMode options or aliases.
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
     * A common strategy across binlog-based connectors to express snapshot locking requirements.
     */
    public interface SnapshotLockingStrategy {
        /**
         * @return whether the snapshot should use locking
         */
        boolean isLockingEnabled(); // useLocking

        /**
         * @return whether minimal locking is enabled
         */
        boolean isMinimalLockingEnabled(); // useMinimalLocking

        /**
         * @return whether the isolation level should be reset on flushes
         */
        boolean isIsolationLevelResetOnFlush(); // flushXXX
    }

    /**
     * Set of all built-in database names that will generally be ignored by the connector.
     */
    protected static final Set<String> BUILT_IN_DB_NAMES = Collect.unmodifiableSet(
            "mysql", "performance_schema", "sys", "information_schema");

    /**
     * The default port for the binlog-based database.
     */
    private static final int DEFAULT_PORT = 3306;

    /**
     * The default size of the binlog buffer used for examining transactions and deciding whether to
     * propagate them or not. A size of {@code 0} disables the buffer, all events will be passed
     * directly as they are passed by the binlog client.
     */
    private static final int DEFAULT_BINLOG_BUFFER_SIZE = 0;

    public static final Field PORT = RelationalDatabaseConnectorConfig.PORT
            .withDefault(DEFAULT_PORT);

    public static final Field TABLES_IGNORE_BUILTIN = RelationalDatabaseConnectorConfig.TABLE_IGNORE_BUILTIN
            .withDependents(DATABASE_INCLUDE_LIST_NAME);

    public static final Field STORE_ONLY_CAPTURED_DATABASES_DDL = HistorizedRelationalDatabaseConnectorConfig.STORE_ONLY_CAPTURED_DATABASES_DDL
            .withDefault(true);

    public static final Field ON_CONNECT_STATEMENTS = Field.create("database.initial.statements")
            .withDisplayName("Initial statements")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("A semicolon separated list of SQL statements to be executed when a JDBC connection "
                    + "(not binlog reading connection) to the database is established. Note that the connector may "
                    + "establish JDBC connections at its own discretion, so this should typically be used for "
                    + "configuration of session parameters only, but not for executing DML statements. Use doubled "
                    + "semicolon (';;') to use a semicolon as a character and not as a delimiter.");

    public static final Field SERVER_ID = Field.create("database.server.id")
            .withDisplayName("Cluster ID")
            .withType(ConfigDef.Type.LONG)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .required()
            .withValidation(Field::isPositiveLong)
            .withDescription("A numeric ID of this database client, which must be unique across all "
                    + "currently-running database processes in the cluster. This connector joins the "
                    + "database cluster as another server (with this unique ID) so it can read the binlog.");

    public static final Field SERVER_ID_OFFSET = Field.create("database.server.id.offset")
            .withDisplayName("Cluster ID offset")
            .withType(ConfigDef.Type.LONG)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDefault(10000L)
            .withDescription("Only relevant if parallel snapshotting is configured. During parallel snapshotting, "
                    + "multiple (4) connections open to the database client, and they each need their own unique "
                    + "connection ID. This offset is used to generate those IDs from the base configured cluster ID.");

    public static final Field SSL_MODE = Field.create("database.ssl.mode")
            .withDisplayName("SSL mode")
            .withEnum(SecureConnectionMode.class, SecureConnectionMode.PREFERRED)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Whether to use an encrypted connection to the database. Options include: "
                    + "'disabled' to use an unencrypted connection; "
                    + "'preferred' (the default) to establish a secure (encrypted) connection if the server supports "
                    + "secure connections, but fall back to an unencrypted connection otherwise; "
                    + "'required' to use a secure (encrypted) connection, and fail if one cannot be established; "
                    + "'verify_ca' like 'required' but additionally verify the server TLS certificate against the "
                    + "configured Certificate Authority (CA) certificates, or fail if no valid matching CA certificates are found; or "
                    + "'verify_identity' like 'verify_ca' but additionally verify that the server certificate matches "
                    + "the host to which the connection is attempted.");

    public static final Field SSL_KEYSTORE = Field.create("database.ssl.keystore")
            .withDisplayName("SSL Keystore")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("The location of the key store file. "
                    + "This is optional and can be used for two-way authentication between the client and the database.");

    public static final Field SSL_KEYSTORE_PASSWORD = Field.create("database.ssl.keystore.password")
            .withDisplayName("SSL Keystore Password")
            .withType(ConfigDef.Type.PASSWORD)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("The password for the key store file. "
                    + "This is optional and only needed if 'database.ssl.keystore' is configured.");

    public static final Field SSL_TRUSTSTORE = Field.create("database.ssl.truststore")
            .withDisplayName("SSL Truststore")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("The location of the trust store file for the server certificate verification.");

    public static final Field SSL_TRUSTSTORE_PASSWORD = Field.create("database.ssl.truststore.password")
            .withDisplayName("SSL Truststore Password")
            .withType(ConfigDef.Type.PASSWORD)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("The password for the trust store file. "
                    + "Used to check the integrity of the truststore, and unlock the truststore.");

    public static final Field CONNECTION_TIMEOUT_MS = Field.create("connect.timeout.ms")
            .withDisplayName("Connection Timeout (ms)")
            .withType(ConfigDef.Type.INT)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDefault(30000)
            .withValidation(Field::isPositiveInteger)
            .withDescription("Maximum time to wait after trying to connect to the database before timing out, "
                    + "given in milliseconds. Defaults to 30 seconds (30,000 ms).");

    public static final Field KEEP_ALIVE = Field.create("connect.keep.alive")
            .withDisplayName("Keep connection alive (true/false)")
            .withType(ConfigDef.Type.BOOLEAN)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(true)
            .withValidation(Field::isBoolean)
            .withDescription("Whether a separate thread should be used to ensure the connection is kept alive.");

    public static final Field KEEP_ALIVE_INTERVAL_MS = Field.create("connect.keep.alive.interval.ms")
            .withDisplayName("Keep alive interval (ms)")
            .withType(ConfigDef.Type.LONG)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(Duration.ofMinutes(1).toMillis())
            .withValidation(Field::isPositiveInteger)
            .withDescription("Interval for connection checking if keep alive thread is used, given in milliseconds "
                    + "Defaults to 1 minute (60,000 ms).");

    public static final Field ROW_COUNT_FOR_STREAMING_RESULT_SETS = Field.create("min.row.count.to.stream.results")
            .withDisplayName("Stream result set of size")
            .withType(ConfigDef.Type.INT)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(1_000)
            .withValidation(Field::isNonNegativeLong)
            .withDescription("The number of rows a table must contain to stream results rather than pull "
                    + "all into memory during snapshots. Defaults to 1,000. Use 0 to stream all results "
                    + "and completely avoid checking the size of each table.");

    public static final Field BUFFER_SIZE_FOR_BINLOG_READER = Field.create("binlog.buffer.size")
            .withDisplayName("Binlog reader buffer size")
            .withType(ConfigDef.Type.INT)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDefault(DEFAULT_BINLOG_BUFFER_SIZE)
            .withValidation(Field::isNonNegativeInteger)
            .withDescription("The size of a look-ahead buffer used by the binlog reader to decide whether the "
                    + "transaction in progress is going to be committed or rolled back. Use 0 to disable look-ahead "
                    + "buffering. Defaults to " + DEFAULT_BINLOG_BUFFER_SIZE + " (i.e. buffering is disabled.");

    public static final Field TOPIC_NAMING_STRATEGY = Field.create("topic.naming.strategy")
            .withDisplayName("Topic naming strategy class")
            .withType(ConfigDef.Type.CLASS)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDefault(DefaultTopicNamingStrategy.class.getName())
            .withDescription("The name of the TopicNamingStrategy class that should be used to determine the topic "
                    + "name for data change, schema change, transaction, heartbeat event etc.");

    public static final Field INCLUDE_SQL_QUERY = Field.create("include.query")
            .withDisplayName("Include original SQL query with in change events")
            .withType(ConfigDef.Type.BOOLEAN)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDefault(false)
            .withDescription("Whether the connector should include the original SQL query that generated the change "
                    + "event. Note: This option requires the database to be configured using the server option "
                    + "binlog_rows_query_log_events (MySQL) or binlog_annotate_row_events (MariaDB) set to ON."
                    + "Query will not be present for events generated from snapshot. WARNING: Enabling this option "
                    + "may expose tables or fields explicitly excluded or masked by including the original SQL "
                    + "statement in the change event. For this reason the default value is 'false'.");

    public static final Field SNAPSHOT_MODE = Field.create("snapshot.mode")
            .withDisplayName("Snapshot mode")
            .withEnum(SnapshotMode.class, SnapshotMode.INITIAL)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("The criteria for running a snapshot upon startup of the connector. "
                    + "Select one of the following snapshot options: "
                    + "'when_needed': On startup, the connector runs a snapshot if one is needed.; "
                    + "'schema_only': If the connector does not detect any offsets for the logical server name, it runs a snapshot that captures only the schema (table structures), but not any table data. After the snapshot completes, the connector begins to stream changes from the binlog.; "
                    + "'schema_only_recovery': The connector performs a snapshot that captures only the database schema history. The connector then transitions back to streaming. Use this setting to restore a corrupted or lost database schema history topic. Do not use if the database schema was modified after the connector stopped.; "
                    + "'initial' (default): If the connector does not detect any offsets for the logical server name, it runs a snapshot that captures the current full state of the configured tables. After the snapshot completes, the connector begins to stream changes from the binlog.; "
                    + "'initial_only': The connector performs a snapshot as it does for the 'initial' option, but after the connector completes the snapshot, it stops, and does not stream changes from the binlog.; "
                    + "'never': The connector does not run a snapshot. Upon first startup, the connector immediately begins reading from the beginning of the binlog. "
                    + "The 'never' mode should be used with care, and only when the binlog is known to contain all history.");

    public static final Field TIME_PRECISION_MODE = RelationalDatabaseConnectorConfig.TIME_PRECISION_MODE
            .withDisplayName("The time precision mode to be used")
            .withEnum(TemporalPrecisionMode.class, TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS)
            .withValidation(BinlogConnectorConfig::validateTimePrecisionMode)
            .withDescription("Time, date and timestamps can be represented with different kinds of precisions, including: "
                    + "'adaptive_time_microseconds': the precision of date and timestamp values is based the database column's precision; but time fields always use microseconds precision; "
                    + "'connect': always represents time, date and timestamp values using Kafka Connect's built-in representations for Time, Date, and Timestamp, "
                    + "which uses millisecond precision regardless of the database columns' precision.");

    public static final Field BIGINT_UNSIGNED_HANDLING_MODE = Field.create("bigint.unsigned.handling.mode")
            .withDisplayName("The mode for handling bigint-unsigned values")
            .withEnum(BigIntUnsignedHandlingMode.class, BigIntUnsignedHandlingMode.LONG)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Specify how BIGINT UNSIGNED columns should be represented in change events, including: "
                    + "'precise' uses java.math.BigDecimal to represent values, which are encoded in the change events using a binary representation and Kafka Connect's 'org.apache.kafka.connect.data.Decimal' type; "
                    + "'long' (the default) represents values using Java's 'long', which may not offer the precision but will be far easier to use in consumers.");

    /**
     * @deprecated Use {@link #EVENT_PROCESSING_FAILURE_HANDLING_MODE} instead, will be removed in Debezium 3.0.
     */
    @Deprecated
    public static final Field EVENT_DESERIALIZATION_FAILURE_HANDLING_MODE = Field.create("event.deserialization.failure.handling.mode")
            .withDisplayName("Event deserialization failure handling")
            .withEnum(EventProcessingFailureHandlingMode.class, EventProcessingFailureHandlingMode.FAIL)
            .withValidation(BinlogConnectorConfig::validateEventDeserializationFailureHandlingModeNotSet)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Specify how failures during deserialization of binlog events (i.e. when encountering a corrupted event) should be handled, including: "
                    + "'fail' (the default) an exception indicating the problematic event and its binlog position is raised, causing the connector to be stopped; "
                    + "'warn' the problematic event and its binlog position will be logged and the event will be skipped; "
                    + "'ignore' the problematic event will be skipped.");

    public static final Field INCONSISTENT_SCHEMA_HANDLING_MODE = Field.create("inconsistent.schema.handling.mode")
            .withDisplayName("Inconsistent schema failure handling")
            .withEnum(EventProcessingFailureHandlingMode.class, EventProcessingFailureHandlingMode.FAIL)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Specify how binlog events that belong to a table missing from internal schema representation "
                    + "(i.e. internal representation is not consistent with database) should be handled, including: "
                    + "'fail' (the default) an exception indicating the problematic event and its binlog position is raised, causing the connector to be stopped; "
                    + "'warn' the problematic event and its binlog position will be logged and the event will be skipped; "
                    + "'skip' the problematic event will be skipped.");

    public static final Field ENABLE_TIME_ADJUSTER = Field.create("enable.time.adjuster")
            .withDisplayName("Enable Time Adjuster")
            .withType(ConfigDef.Type.BOOLEAN)
            .withDefault(true)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("The database allows the user to insert year value as either 2-digit or 4-digit. "
                    + "In case of two digit the value is automatically mapped into 1970 - 2069."
                    + "false - delegates the implicit conversion to the database; "
                    + "true - (the default) Debezium makes the conversion");

    public static final Field READ_ONLY_CONNECTION = Field.create("read.only")
            .withDisplayName("Read only connection")
            .withType(ConfigDef.Type.BOOLEAN)
            .withDefault(false)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Switched connector to use alternative methods to deliver signals to Debezium instead "
                    + "of writing to signaling table");

    /**
     * Specifies the GTID ranges to be included.<p></p>
     *
     * Each binlog-database implementation has their own GTID specification, please see the binlog conenctor
     * concrete implementation for details about the GTID inclusion range format.
     */
    public static final Field GTID_SOURCE_INCLUDES = Field.create("gtid.source.includes")
            .withDisplayName("Include GTID sources")
            .withType(ConfigDef.Type.LIST)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDependents(TABLE_INCLUDE_LIST_NAME);

    /**
     * Specifies the GTID ranges to be excluded.<p></p>
     *
     * Each binlog-database implementation has their own GTID specification, please see the binlog connector
     * concrete implementation for details about the GTID exclusion range format.
     */
    public static final Field GTID_SOURCE_EXCLUDES = Field.create("gtid.source.excludes")
            .withDisplayName("Exclude GTID sources")
            .withType(ConfigDef.Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 25))
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withValidation(BinlogConnectorConfig::validateGtidSetExcludes)
            .withInvisibleRecommender();

    /**
     * When set to {@code true}, the connector will produce DML events for transactions that were written by
     * the binlog-based database servers based on the matching GTID filters that were included in the connector
     * configuration in {@link #GTID_SOURCE_INCLUDES} and {@link #GTID_SOURCE_EXCLUDES}, if specified.<p></p>
     *
     * This defaults to {@code true}, which requires that either {@link #GTID_SOURCE_INCLUDES} or
     * {@link #GTID_SOURCE_EXCLUDES} must be provided in the connector configuration.
     */
    public static final Field GTID_SOURCE_FILTER_DML_EVENTS = Field.create("gtid.source.filter.dml.events")
            .withDisplayName("Filter DML events")
            .withType(ConfigDef.Type.BOOLEAN)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 23))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDefault(true)
            .withDescription("When set to true, only produce DML events for transactions that were written on the "
                    + "server with matching GTIDs defined by the `gtid.source.includes` or `gtid.source.excludes`, "
                    + "if they were specified.");

    protected static final ConfigDefinition CONFIG_DEFINITION = HistorizedRelationalDatabaseConnectorConfig.CONFIG_DEFINITION.edit()
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
                    SSL_TRUSTSTORE_PASSWORD)
            .connector(
                    CONNECTION_TIMEOUT_MS,
                    KEEP_ALIVE,
                    KEEP_ALIVE_INTERVAL_MS,
                    SNAPSHOT_MODE,
                    SNAPSHOT_QUERY_MODE,
                    SNAPSHOT_QUERY_MODE_CUSTOM_NAME,
                    BIGINT_UNSIGNED_HANDLING_MODE,
                    TIME_PRECISION_MODE,
                    ENABLE_TIME_ADJUSTER,
                    SCHEMA_NAME_ADJUSTMENT_MODE,
                    ROW_COUNT_FOR_STREAMING_RESULT_SETS,
                    INCREMENTAL_SNAPSHOT_CHUNK_SIZE,
                    INCREMENTAL_SNAPSHOT_ALLOW_SCHEMA_CHANGES,
                    STORE_ONLY_CAPTURED_DATABASES_DDL)
            .events(
                    INCLUDE_SQL_QUERY,
                    TABLES_IGNORE_BUILTIN,
                    DATABASE_INCLUDE_LIST,
                    DATABASE_EXCLUDE_LIST,
                    BUFFER_SIZE_FOR_BINLOG_READER,
                    EVENT_DESERIALIZATION_FAILURE_HANDLING_MODE,
                    INCONSISTENT_SCHEMA_HANDLING_MODE,
                    GTID_SOURCE_INCLUDES,
                    GTID_SOURCE_EXCLUDES,
                    GTID_SOURCE_FILTER_DML_EVENTS)
            .create();

    private final Configuration config;
    private final SnapshotMode snapshotMode;
    private final TemporalPrecisionMode temporalPrecisionMode;
    private final Duration connectionTimeout;
    private final EventProcessingFailureHandlingMode inconsistentSchemaFailureHandlingMode;
    private final SecureConnectionMode secureConnectionMode;
    private final BigIntUnsignedHandlingMode bigIntUnsignedHandlingMode;
    private final boolean readOnlyConnection;

    /**
     * Create a binlog-based connector configuration.
     *
     * @param connectorClazz the connector class the configuration refers
     * @param config the configuration
     * @param defaultFetchSize the default fetch size to be used
     */
    public BinlogConnectorConfig(Class<? extends SourceConnector> connectorClazz, Configuration config, int defaultFetchSize) {
        super(connectorClazz,
                config,
                Tables.TableFilter.fromPredicate(BinlogConnectorConfig::isNotBuiltInTable),
                true,
                defaultFetchSize,
                ColumnFilterMode.CATALOG,
                false);

        this.config = config;
        this.temporalPrecisionMode = TemporalPrecisionMode.parse(config.getString(TIME_PRECISION_MODE));
        this.snapshotMode = SnapshotMode.parse(config.getString(SNAPSHOT_MODE), SNAPSHOT_MODE.defaultValueAsString());
        this.readOnlyConnection = config.getBoolean(READ_ONLY_CONNECTION);
        this.storeOnlyCapturedDatabasesDdl = config.getBoolean(STORE_ONLY_CAPTURED_DATABASES_DDL);
        this.connectionTimeout = Duration.ofMillis(config.getLong(CONNECTION_TIMEOUT_MS));
        this.inconsistentSchemaFailureHandlingMode = EventProcessingFailureHandlingMode.parse(config.getString(INCONSISTENT_SCHEMA_HANDLING_MODE));
        this.secureConnectionMode = SecureConnectionMode.parse(config.getString(SSL_MODE));
        this.bigIntUnsignedHandlingMode = BigIntUnsignedHandlingMode.parse(config.getString(BIGINT_UNSIGNED_HANDLING_MODE));
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
    public TemporalPrecisionMode getTemporalPrecisionMode() {
        return temporalPrecisionMode;
    }

    /**
     * @return which snapshot mode the connector intends to use
     */
    public SnapshotMode getSnapshotMode() {
        return snapshotMode;
    }

    /**
     * @return the connection timeout duration
     */
    public Duration getConnectionTimeout() {
        return connectionTimeout;
    }

    /**
     * @return how inconsistent schema failures should be handled
     */
    public EventProcessingFailureHandlingMode getInconsistentSchemaFailureHandlingMode() {
        return inconsistentSchemaFailureHandlingMode;
    }

    /**
     * @return the {@code BIGINT UNSIGNED} handling mode
     */
    public BigIntUnsignedHandlingMode getBigIntUnsignedHandlingMode() {
        return bigIntUnsignedHandlingMode;
    }

    /**
     * Specifies whether the connector should use a cursor-fetch to read result sets.
     *
     * @return true to use a cursor-fetch; false otherwise
     */
    public boolean useCursorFetch() {
        return getSnapshotFetchSize() > 0;
    }

    /**
     * @return the database server host name
     */
    public String getHostName() {
        return config.getString(HOSTNAME);
    }

    /**
     * @return the database server port
     */
    public int getPort() {
        return config.getInteger(PORT);
    }

    /**
     * @return the database connection username
     */
    public String getUserName() {
        return config.getString(USER);
    }

    /**
     * @return the database connection credentials
     */
    public String getPassword() {
        return config.getString(PASSWORD);
    }

    /**
     * @return the database cluster server unique identifier
     */
    public long getServerId() {
        return config.getInteger(SERVER_ID);
    }

    /**
     * @return the SSL connection mode to use
     */
    public SecureConnectionMode getSslMode() {
        return secureConnectionMode;
    }

    /**
     * @return true if the connection should use SSL; false otherwise
     */
    public boolean isSslModeEnabled() {
        return secureConnectionMode != SecureConnectionMode.DISABLED;
    }

    /**
     * @return the buffer size for streaming change events
     */
    public int getBufferSizeForStreamingChangeEventSource() {
        return config.getInteger(BUFFER_SIZE_FOR_BINLOG_READER);
    }

    /**
     * @return whether the SQL query for a binlog event should be included in the event payload
     */
    public boolean isSqlQueryIncluded() {
        return config.getBoolean(INCLUDE_SQL_QUERY);
    }

    /**
     * @return the number of rows a table needs to stream results rather than read all into memory.
     */
    public long getRowCountForLargeTable() {
        return config.getLong(ROW_COUNT_FOR_STREAMING_RESULT_SETS);
    }

    /**
     * @return whether database connection should be treated as read-only.
     */
    public boolean isReadOnlyConnection() {
        return readOnlyConnection;
    }

    /**
     * @return true if the time adjuster is enabled; false otherwise
     */
    public boolean isTimeAdjustedEnabled() {
        return config.getBoolean(ENABLE_TIME_ADJUSTER);
    }

    /**
     * @return the global transaction identifier source filter predicate
     */
    public abstract Predicate<String> getGtidSourceFilter();

    /**
     * @return the global transaction identifier set ({@link GtidSet} factory.
     */
    public abstract GtidSetFactory getGtidSetFactory();

    /**
     * Check whether tests request global lock usage.
     *
     * @return true to use global locks, false otherwise
     */
    protected boolean isGlobalLockUseRequested() {
        return !"true".equals(config.getString(TEST_DISABLE_GLOBAL_LOCKING));
    }

    /**
     * Check whether the specified database name is a built-in database.
     *
     * @param databaseName the database name to check
     * @return true if the database is a built-in database; false otherwise
     */
    public static boolean isBuiltInDatabase(String databaseName) {
        if (databaseName == null) {
            return false;
        }
        return BUILT_IN_DB_NAMES.contains(databaseName.toLowerCase());
    }

    /**
     * Checks whether the {@link TableId} refers to a built-in table.
     *
     * @param tableId the relational table identifier, should not be null
     * @return true if the reference refers to a built-in table
     */
    public static boolean isNotBuiltInTable(TableId tableId) {
        return !isBuiltInDatabase(tableId.catalog());
    }

    /**
     * @return the connector-specific's {@link SnapshotLockingStrategy}.
     */
    protected abstract SnapshotLockingStrategy getSnapshotLockingStrategy();

    /**
     * Logs a deprecation warning when using {@link #EVENT_DESERIALIZATION_FAILURE_HANDLING_MODE}.
     */
    private static int validateEventDeserializationFailureHandlingModeNotSet(Configuration config, Field field, ValidationOutput problems) {
        final String modeName = config.asMap().get(EVENT_DESERIALIZATION_FAILURE_HANDLING_MODE.name());
        if (modeName != null) {
            LOGGER.warn("Configuration option '{}' is deprecated, use to '{}' instead.",
                    EVENT_DESERIALIZATION_FAILURE_HANDLING_MODE.name(),
                    EVENT_PROCESSING_FAILURE_HANDLING_MODE.name());
        }
        return 0;
    }

    /**
     * Validate the time.precision.mode configuration.<p></p>
     *
     * If {@code adaptive} is specified, this option has the potential to cause overflow which is why the
     * option was deprecated and no longer supported for this connector.
     */
    private static int validateTimePrecisionMode(Configuration config, Field field, ValidationOutput problems) {
        if (config.hasKey(TIME_PRECISION_MODE.name())) {
            final String timePrecisionMode = config.getString(TIME_PRECISION_MODE.name());
            if (TemporalPrecisionMode.ADAPTIVE.getValue().equals(timePrecisionMode)) {
                problems.accept(TIME_PRECISION_MODE, timePrecisionMode, "The 'adaptive' time.precision.mode is no longer supported");
                return 1;
            }
        }
        return 0;
    }

    /**
     * Validate that GTID excludes aren't provided when GTID includes are in the configuration.
     */
    private static int validateGtidSetExcludes(Configuration config, Field field, ValidationOutput problems) {
        String includes = config.getString(GTID_SOURCE_INCLUDES);
        String excludes = config.getString(GTID_SOURCE_EXCLUDES);
        if (includes != null && excludes != null) {
            problems.accept(GTID_SOURCE_EXCLUDES, excludes, "Cannot specify GTID excludes when includes are provided.");
            return 1;
        }
        return 0;
    }
}
