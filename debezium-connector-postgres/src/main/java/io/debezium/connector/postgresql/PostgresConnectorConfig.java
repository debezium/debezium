/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.connector.postgresql.connection.MessageDecoder;
import io.debezium.connector.postgresql.connection.MessageDecoderContext;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.connection.pgoutput.PgOutputMessageDecoder;
import io.debezium.connector.postgresql.connection.pgproto.PgProtoMessageDecoder;
import io.debezium.connector.postgresql.snapshot.AlwaysSnapshotter;
import io.debezium.connector.postgresql.snapshot.InitialOnlySnapshotter;
import io.debezium.connector.postgresql.snapshot.InitialSnapshotter;
import io.debezium.connector.postgresql.snapshot.NeverSnapshotter;
import io.debezium.connector.postgresql.spi.Snapshotter;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.util.Strings;

/**
 * The configuration properties for the {@link PostgresConnector}
 *
 * @author Horia Chiorean
 */
public class PostgresConnectorConfig extends RelationalDatabaseConnectorConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresConnectorConfig.class);

    /**
     * The set of predefined HStoreHandlingMode options or aliases
     */
    public enum HStoreHandlingMode implements EnumeratedValue {

        /**
         * Represents HStore value as json
         */
        JSON("json"),

        /**
         * Represents HStore value as map
         */
        MAP("map");

        private final String value;

        HStoreHandlingMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied values is one of the predefined options
         *
         * @param value the configuration property value ; may not be null
         * @return the matching option, or null if the match is not found
         */
        public static HStoreHandlingMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (HStoreHandlingMode option : HStoreHandlingMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        /**
         * Determine if the supplied values is one of the predefined options
         *
         * @param value the configuration property value ; may not be null
         * @param defaultValue the default value ; may be null
         * @return the matching option or null if the match is not found and non-null default is invalid
         */
        public static HStoreHandlingMode parse(String value, String defaultValue) {
            HStoreHandlingMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    /**
     * Defines modes of representation of {@code interval} datatype
     */
    public enum IntervalHandlingMode implements EnumeratedValue {

        /**
         * Represents interval as inexact microseconds count
         */
        NUMERIC("numeric"),

        /**
         * Represents interval as ISO 8601 time interval
         */
        STRING("string");

        private final String value;

        IntervalHandlingMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Convert mode name into the logical value
         *
         * @param value the configuration property value ; may not be null
         * @return the matching option, or null if the match is not found
         */
        public static IntervalHandlingMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (IntervalHandlingMode option : IntervalHandlingMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        /**
         * Convert mode name into the logical value
         *
         * @param value the configuration property value ; may not be null
         * @param defaultValue the default value ; may be null
         * @return the matching option or null if the match is not found and non-null default is invalid
         */
        public static IntervalHandlingMode parse(String value, String defaultValue) {
            IntervalHandlingMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    /**
     * The set of predefined Snapshotter options or aliases.
     */
    public enum SnapshotMode implements EnumeratedValue {

        /**
         * Always perform a snapshot when starting.
         */
        ALWAYS("always", (c) -> new AlwaysSnapshotter()),

        /**
         * Perform a snapshot only upon initial startup of a connector.
         */
        INITIAL("initial", (c) -> new InitialSnapshotter()),

        /**
         * Never perform a snapshot and only receive logical changes.
         */
        NEVER("never", (c) -> new NeverSnapshotter()),

        /**
         * Perform a snapshot and then stop before attempting to receive any logical changes.
         */
        INITIAL_ONLY("initial_only", (c) -> new InitialOnlySnapshotter()),

        /**
         * Inject a custom snapshotter, which allows for more control over snapshots.
         */
        CUSTOM("custom", (c) -> {
            return c.getInstance(SNAPSHOT_MODE_CLASS, Snapshotter.class);
        });

        @FunctionalInterface
        public interface SnapshotterBuilder {
            Snapshotter buildSnapshotter(Configuration config);
        }

        private final String value;
        private final SnapshotterBuilder builderFunc;

        SnapshotMode(String value, SnapshotterBuilder buildSnapshotter) {
            this.value = value;
            this.builderFunc = buildSnapshotter;
        }

        public Snapshotter getSnapshotter(Configuration config) {
            return builderFunc.buildSnapshotter(config);
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
         * Establish an unencrypted connection
         *
         * see the {@code sslmode} Postgres JDBC driver option
         */
        DISABLED("disable"),

        /**
         * Establish a secure connection if the server supports secure connections.
         * The connection attempt fails if a secure connection cannot be established
         *
         * see the {@code sslmode} Postgres JDBC driver option
         */
        REQUIRED("require"),

        /**
         * Like REQUIRED, but additionally verify the server TLS certificate against the configured Certificate Authority
         * (CA) certificates. The connection attempt fails if no valid matching CA certificates are found.
         *
         * see the {@code sslmode} Postgres JDBC driver option
         */
        VERIFY_CA("verify-ca"),

        /**
         * Like VERIFY_CA, but additionally verify that the server certificate matches the host to which the connection is
         * attempted.
         *
         * see the {@code sslmode} Postgres JDBC driver option
         */
        VERIFY_FULL("verify-full");

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

    public enum LogicalDecoder implements EnumeratedValue {
        PGOUTPUT("pgoutput") {
            @Override
            public MessageDecoder messageDecoder(MessageDecoderContext config, PostgresConnection connection) {
                return new PgOutputMessageDecoder(config, connection);
            }

            @Override
            public String getPostgresPluginName() {
                return getValue();
            }

            @Override
            public boolean supportsTruncate() {
                return true;
            }

            @Override
            public boolean supportsLogicalDecodingMessage() {
                return true;
            }
        },
        DECODERBUFS("decoderbufs") {
            @Override
            public MessageDecoder messageDecoder(MessageDecoderContext config, PostgresConnection connection) {
                return new PgProtoMessageDecoder();
            }

            @Override
            public String getPostgresPluginName() {
                return getValue();
            }

            @Override
            public boolean supportsTruncate() {
                return false;
            }

            @Override
            public boolean supportsLogicalDecodingMessage() {
                return false;
            }
        };

        private final String decoderName;

        LogicalDecoder(String decoderName) {
            this.decoderName = decoderName;
        }

        public abstract MessageDecoder messageDecoder(MessageDecoderContext config, PostgresConnection connection);

        public static LogicalDecoder parse(String s) {
            return valueOf(s.trim().toUpperCase());
        }

        @Override
        public String getValue() {
            return decoderName;
        }

        public abstract String getPostgresPluginName();

        public abstract boolean supportsTruncate();

        public abstract boolean supportsLogicalDecodingMessage();
    }

    /**
     * The set of predefined SchemaRefreshMode options or aliases.
     */
    public enum SchemaRefreshMode implements EnumeratedValue {
        /**
         * Refresh the in-memory schema cache whenever there is a discrepancy between it and the schema derived from the
         * incoming message.
         */
        COLUMNS_DIFF("columns_diff"),

        /**
         * Refresh the in-memory schema cache if there is a discrepancy between it and the schema derived from the
         * incoming message, unless TOASTable data can account for the discrepancy.
         *
         * This setting can improve connector performance significantly if there are frequently-updated tables that
         * have TOASTed data that are rarely part of these updates. However, it is possible for the in-memory schema to
         * become outdated if TOASTable columns are dropped from the table.
         */
        COLUMNS_DIFF_EXCLUDE_UNCHANGED_TOAST("columns_diff_exclude_unchanged_toast");

        private final String value;

        SchemaRefreshMode(String value) {
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
        public static SchemaRefreshMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (SchemaRefreshMode option : SchemaRefreshMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }
    }

    protected static final String DATABASE_CONFIG_PREFIX = "database.";
    protected static final int DEFAULT_PORT = 5_432;
    protected static final int DEFAULT_SNAPSHOT_FETCH_SIZE = 10_240;
    protected static final int DEFAULT_MAX_RETRIES = 6;

    public static final Field PORT = RelationalDatabaseConnectorConfig.PORT
            .withDefault(DEFAULT_PORT);

    public static final Field PLUGIN_NAME = Field.create("plugin.name")
            .withDisplayName("Plugin")
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_REPLICATION, 0))
            .withEnum(LogicalDecoder.class, LogicalDecoder.DECODERBUFS)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDescription("The name of the Postgres logical decoding plugin installed on the server. " +
                    "Supported values are '" + LogicalDecoder.DECODERBUFS.getValue()
                    + "' and '" + LogicalDecoder.PGOUTPUT.getValue()
                    + "'. " +
                    "Defaults to '" + LogicalDecoder.DECODERBUFS.getValue() + "'.");

    public static final Field SLOT_NAME = Field.create("slot.name")
            .withDisplayName("Slot")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_REPLICATION, 1))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDefault(ReplicationConnection.Builder.DEFAULT_SLOT_NAME)
            .withValidation(PostgresConnectorConfig::validateReplicationSlotName)
            .withDescription("The name of the Postgres logical decoding slot created for streaming changes from a plugin. " +
                    "Defaults to 'debezium");

    public static final Field DROP_SLOT_ON_STOP = Field.create("slot.drop.on.stop")
            .withDisplayName("Drop slot on stop")
            .withType(Type.BOOLEAN)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_REPLICATION, 3))
            .withDefault(false)
            .withImportance(Importance.MEDIUM)
            .withDescription(
                    "Whether or not to drop the logical replication slot when the connector finishes orderly. " +
                            "By default the replication is kept so that on restart progress can resume from the last recorded location");

    public static final Field PUBLICATION_NAME = Field.create("publication.name")
            .withDisplayName("Publication")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_REPLICATION, 8))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDefault(ReplicationConnection.Builder.DEFAULT_PUBLICATION_NAME)
            .withDescription("The name of the Postgres 10+ publication used for streaming changes from a plugin. " +
                    "Defaults to '" + ReplicationConnection.Builder.DEFAULT_PUBLICATION_NAME + "'");

    public enum AutoCreateMode implements EnumeratedValue {
        /**
         * No Publication will be created, it's expected the user
         * has already created the publication.
         */
        DISABLED("disabled"),
        /**
         * Enable publication for all tables.
         */
        ALL_TABLES("all_tables"),
        /**
         * Enable publication on a specific set of tables.
         */
        FILTERED("filtered");

        private final String value;

        AutoCreateMode(String value) {
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
        public static AutoCreateMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (AutoCreateMode option : AutoCreateMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value        the configuration property value; may not be null
         * @param defaultValue the default value; may be null
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static AutoCreateMode parse(String value, String defaultValue) {
            AutoCreateMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    public static final Field PUBLICATION_AUTOCREATE_MODE = Field.create("publication.autocreate.mode")
            .withDisplayName("Publication Auto Create Mode")
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_REPLICATION, 9))
            .withEnum(AutoCreateMode.class, AutoCreateMode.ALL_TABLES)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDescription(
                    "Applies only when streaming changes using pgoutput." +
                            "Determine how creation of a publication should work, the default is all_tables." +
                            "DISABLED - The connector will not attempt to create a publication at all. The expectation is " +
                            "that the user has created the publication up-front. If the publication isn't found to exist upon " +
                            "startup, the connector will throw an exception and stop." +
                            "ALL_TABLES - If no publication exists, the connector will create a new publication for all tables. " +
                            "Note this requires that the configured user has access. If the publication already exists, it will be used" +
                            ". i.e CREATE PUBLICATION <publication_name> FOR ALL TABLES;" +
                            "FILTERED - If no publication exists, the connector will create a new publication for all those tables matching" +
                            "the current filter configuration (see table/database include/exclude list properties). If the publication already" +
                            " exists, it will be used. i.e CREATE PUBLICATION <publication_name> FOR TABLE <tbl1, tbl2, etc>");

    public static final Field STREAM_PARAMS = Field.create("slot.stream.params")
            .withDisplayName("Optional parameters to pass to the logical decoder when the stream is started.")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_REPLICATION, 2))
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withDescription(
                    "Any optional parameters used by logical decoding plugin. Semi-colon separated. E.g. 'add-tables=public.table,public.table2;include-lsn=true'");

    public static final Field MAX_RETRIES = Field.create("slot.max.retries")
            .withDisplayName("Retry count")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_REPLICATION, 4))
            .withImportance(Importance.LOW)
            .withDefault(DEFAULT_MAX_RETRIES)
            .withValidation(Field::isInteger)
            .withDescription("How many times to retry connecting to a replication slot when an attempt fails.");

    public static final Field RETRY_DELAY_MS = Field.create("slot.retry.delay.ms")
            .withDisplayName("Retry delay")
            .withType(Type.LONG)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_REPLICATION, 5))
            .withImportance(Importance.LOW)
            .withDefault(Duration.ofSeconds(10).toMillis())
            .withValidation(Field::isInteger)
            .withDescription(
                    "Time to wait between retry attempts when the connector fails to connect to a replication slot, given in milliseconds. Defaults to 10 seconds (10,000 ms).");

    public static final Field ON_CONNECT_STATEMENTS = Field.create(DATABASE_CONFIG_PREFIX + JdbcConfiguration.ON_CONNECT_STATEMENTS)
            .withDisplayName("Initial statements")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 1))
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withDescription("A semicolon separated list of SQL statements to be executed when a JDBC connection to the database is established. "
                    + "Note that the connector may establish JDBC connections at its own discretion, so this should typically be used for configuration "
                    + "of session parameters only, but not for executing DML statements. Use doubled semicolon (';;') to use a semicolon as a character "
                    + "and not as a delimiter.");

    public static final Field SSL_MODE = Field.create(DATABASE_CONFIG_PREFIX + "sslmode")
            .withDisplayName("SSL mode")
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_SSL, 0))
            .withEnum(SecureConnectionMode.class, SecureConnectionMode.DISABLED)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDescription("Whether to use an encrypted connection to Postgres. Options include: "
                    + "'disable' (the default) to use an unencrypted connection; "
                    + "'require' to use a secure (encrypted) connection, and fail if one cannot be established; "
                    + "'verify-ca' like 'required' but additionally verify the server TLS certificate against the configured Certificate Authority "
                    + "(CA) certificates, or fail if no valid matching CA certificates are found; or "
                    + "'verify-full' like 'verify-ca' but additionally verify that the server certificate matches the host to which the connection is attempted.");

    public static final Field SSL_CLIENT_CERT = Field.create(DATABASE_CONFIG_PREFIX + "sslcert")
            .withDisplayName("SSL Client Certificate")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_SSL, 1))
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDescription("File containing the SSL Certificate for the client. See the Postgres SSL docs for further information");

    public static final Field SSL_CLIENT_KEY = Field.create(DATABASE_CONFIG_PREFIX + "sslkey")
            .withDisplayName("SSL Client Key")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_SSL, 4))
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDescription("File containing the SSL private key for the client. See the Postgres SSL docs for further information");

    public static final Field SSL_CLIENT_KEY_PASSWORD = Field.create(DATABASE_CONFIG_PREFIX + "sslpassword")
            .withDisplayName("SSL Client Key Password")
            .withType(Type.PASSWORD)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_SSL, 2))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDescription("Password to access the client private key from the file specified by 'database.sslkey'. See the Postgres SSL docs for further information");

    public static final Field SSL_ROOT_CERT = Field.create(DATABASE_CONFIG_PREFIX + "sslrootcert")
            .withDisplayName("SSL Root Certificate")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_SSL, 3))
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDescription("File containing the root certificate(s) against which the server is validated. See the Postgres JDBC SSL docs for further information");

    public static final Field SSL_SOCKET_FACTORY = Field.create(DATABASE_CONFIG_PREFIX + "sslfactory")
            .withDisplayName("SSL Root Certificate")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_SSL, 5))
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDescription(
                    "A name of class to that creates SSL Sockets. Use org.postgresql.ssl.NonValidatingFactory to disable SSL validation in development environments");

    public static final Field INCREMENTAL_SNAPSHOT_ISOLATION_MODE = RelationalDatabaseConnectorConfig.INCREMENTAL_SNAPSHOT_ISOLATION_MODE
            .withEnum(SnapshotIsolationMode.class, SnapshotIsolationMode.READ_COMMITTED)
            .withDescription("Controls which transaction isolation level is used and how long the connector locks the captured tables. "
                    + "The default is '" + SnapshotIsolationMode.READ_COMMITTED.getValue()
                    + "', which means that read committed isolation level is used."
                    + "Using a value of '" + SnapshotIsolationMode.SERIALIZABLE.getValue()
                    + "' ensures that the connector holds the exclusive lock (and thus prevents any reads and updates) for captured table during the snapshot duration. "
                    + "When '" + SnapshotIsolationMode.REPEATABLE_READ.getValue()
                    + "' is specified, connector runs the snapshot in repeatable read isolation level. No long-running locks are taken, so that incremental snapshot does not prevent "
                    + "other transactions from updating table rows. Snapshot consistency is not guaranteed."
                    + "In '" + SnapshotIsolationMode.READ_UNCOMMITTED.getValue()
                    + "' mode neither table nor row-level locks are acquired, but connector does not guarantee snapshot consistency.");

    public static final Field SNAPSHOT_MODE = Field.create("snapshot.mode")
            .withDisplayName("Snapshot mode")
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 0))
            .withEnum(SnapshotMode.class, SnapshotMode.INITIAL)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("The criteria for running a snapshot upon startup of the connector. "
                    + "Options include: "
                    + "'always' to specify that the connector run a snapshot each time it starts up; "
                    + "'initial' (the default) to specify the connector can run a snapshot only when no offsets are available for the logical server name; "
                    + "'initial_only' same as 'initial' except the connector should stop after completing the snapshot and before it would normally start emitting changes;"
                    + "'never' to specify the connector should never run a snapshot and that upon first startup the connector should read from the last position (LSN) recorded by the server; and"
                    + "'exported' deprecated, use 'initial' instead; "
                    + "'custom' to specify a custom class with 'snapshot.custom_class' which will be loaded and used to determine the snapshot, see docs for more details.");

    public static final Field SNAPSHOT_MODE_CLASS = Field.create("snapshot.custom.class")
            .withDisplayName("Snapshot Mode Custom Class")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 9))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withValidation((config, field, output) -> {
                if (config.getString(SNAPSHOT_MODE).toLowerCase().equals("custom") && config.getString(field).isEmpty()) {
                    output.accept(field, "", "snapshot.custom_class cannot be empty when snapshot.mode 'custom' is defined");
                    return 1;
                }
                return 0;
            })
            .withDescription(
                    "When 'snapshot.mode' is set as custom, this setting must be set to specify a fully qualified class name to load (via the default class loader). "
                            + "This class must implement the 'Snapshotter' interface and is called on each app boot to determine whether to do a snapshot and how to build queries.");

    /**
     * A comma-separated list of regular expressions that match the prefix of logical decoding messages to be excluded
     * from monitoring. Must not be used with {@link #LOGICAL_DECODING_MESSAGE_PREFIX_INCLUDE_LIST}
     */
    public static final Field LOGICAL_DECODING_MESSAGE_PREFIX_EXCLUDE_LIST = Field.create("message.prefix.exclude.list")
            .withDisplayName("Exclude Logical Decoding Message Prefixes")
            .withType(Type.LIST)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 25))
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withValidation(Field::isListOfRegex, PostgresConnectorConfig::validateLogicalDecodingMessageExcludeList)
            .withDescription("A comma-separated list of regular expressions that match the logical decoding message prefixes to be excluded from monitoring.");

    /**
     * A comma-separated list of regular expressions that match the prefix of logical decoding messages to be monitored.
     * Must not be used with {@link #LOGICAL_DECODING_MESSAGE_PREFIX_EXCLUDE_LIST}
     */
    public static final Field LOGICAL_DECODING_MESSAGE_PREFIX_INCLUDE_LIST = Field.create("message.prefix.include.list")
            .withDisplayName("Include Logical Decoding Message Prefixes")
            .withType(Type.LIST)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 24))
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withValidation(Field::isListOfRegex)
            .withDescription(
                    "A comma-separated list of regular expressions that match the logical decoding message prefixes to be monitored. All prefixes are monitored by default.");

    public static final Field HSTORE_HANDLING_MODE = Field.create("hstore.handling.mode")
            .withDisplayName("HStore Handling")
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 22))
            .withEnum(HStoreHandlingMode.class, HStoreHandlingMode.JSON)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDescription("Specify how HSTORE columns should be represented in change events, including: "
                    + "'json' represents values as string-ified JSON (default); "
                    + "'map' represents values as a key/value map");

    public static final Field INTERVAL_HANDLING_MODE = Field.create("interval.handling.mode")
            .withDisplayName("Interval Handling")
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 21))
            .withEnum(IntervalHandlingMode.class, IntervalHandlingMode.NUMERIC)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDescription("Specify how INTERVAL columns should be represented in change events, including: "
                    + "'string' represents values as an exact ISO formatted string; "
                    + "'numeric' (default) represents values using the inexact conversion into microseconds");

    public static final Field STATUS_UPDATE_INTERVAL_MS = Field.create("status.update.interval.ms")
            .withDisplayName("Status update interval (ms)")
            .withType(Type.INT) // Postgres doesn't accept long for this value
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_REPLICATION, 6))
            .withDefault(10_000)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Frequency for sending replication connection status updates to the server, given in milliseconds. Defaults to 10 seconds (10,000 ms).")
            .withValidation(Field::isPositiveInteger);

    public static final Field TCP_KEEPALIVE = Field.create(DATABASE_CONFIG_PREFIX + "tcpKeepAlive")
            .withDisplayName("TCP keep-alive probe")
            .withType(Type.BOOLEAN)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 0))
            .withDefault(true)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Enable or disable TCP keep-alive probe to avoid dropping TCP connection")
            .withValidation(Field::isBoolean);

    public static final Field INCLUDE_UNKNOWN_DATATYPES = Field.create("include.unknown.datatypes")
            .withDisplayName("Include unknown datatypes")
            .withType(Type.BOOLEAN)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 1))
            .withDefault(false)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Specify whether the fields of data type not supported by Debezium should be processed: "
                    + "'false' (the default) omits the fields; "
                    + "'true' converts the field into an implementation dependent binary representation.");

    public static final Field SCHEMA_REFRESH_MODE = Field.create("schema.refresh.mode")
            .withDisplayName("Schema refresh mode")
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 0))
            .withEnum(SchemaRefreshMode.class, SchemaRefreshMode.COLUMNS_DIFF)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Specify the conditions that trigger a refresh of the in-memory schema for a table. " +
                    "'columns_diff' (the default) is the safest mode, ensuring the in-memory schema stays in-sync with " +
                    "the database table's schema at all times. " +
                    "'columns_diff_exclude_unchanged_toast' instructs the connector to refresh the in-memory schema cache if there is a discrepancy between it " +
                    "and the schema derived from the incoming message, unless unchanged TOASTable data fully accounts for the discrepancy. " +
                    "This setting can improve connector performance significantly if there are frequently-updated tables that " +
                    "have TOASTed data that are rarely part of these updates. However, it is possible for the in-memory schema to " +
                    "become outdated if TOASTable columns are dropped from the table.");

    public static final Field XMIN_FETCH_INTERVAL = Field.create("xmin.fetch.interval.ms")
            .withDisplayName("Xmin fetch interval (ms)")
            .withType(Type.LONG)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_REPLICATION, 7))
            .withWidth(Width.SHORT)
            .withDefault(0L)
            .withImportance(Importance.MEDIUM)
            .withDescription("Specify how often (in ms) the xmin will be fetched from the replication slot. " +
                    "This xmin value is exposed by the slot which gives a lower bound of where a new replication slot could start from. " +
                    "The lower the value, the more likely this value is to be the current 'true' value, but the bigger the performance cost. " +
                    "The bigger the value, the less likely this value is to be the current 'true' value, but the lower the performance penalty. " +
                    "The default is set to 0 ms, which disables tracking xmin.")
            .withValidation(Field::isNonNegativeLong);

    public static final Field UNAVAILABLE_VALUE_PLACEHOLDER = RelationalDatabaseConnectorConfig.UNAVAILABLE_VALUE_PLACEHOLDER
            .withDescription("Specify the constant that will be provided by Debezium to indicate that " +
                    "the original value is a toasted value not provided by the database. " +
                    "If starts with 'hex:' prefix it is expected that the rest of the string represents hexadecimal encoded octets.");

    public static final Field MONEY_FRACTION_DIGITS = Field.create("money.fraction.digits")
            .withDisplayName("Money fraction digits")
            .withType(Type.SHORT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 1))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDefault(2)
            .withDescription("Number of fractional digits when money type is converted to 'precise' decimal number.");

    public static final Field SHOULD_FLUSH_LSN_IN_SOURCE_DB = Field.create("flush.lsn.source")
            .withDisplayName("Boolean to determine if Debezium should flush LSN in the source database")
            .withType(Type.BOOLEAN)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 99))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription(
                    "Boolean to determine if Debezium should flush LSN in the source postgres database. If set to false, user will have to flush the LSN manually outside Debezium.")
            .withDefault(Boolean.TRUE)
            .withValidation(Field::isBoolean, PostgresConnectorConfig::validateFlushLsnSource);

    private final LogicalDecodingMessageFilter logicalDecodingMessageFilter;
    private final HStoreHandlingMode hStoreHandlingMode;
    private final IntervalHandlingMode intervalHandlingMode;
    private final SnapshotMode snapshotMode;
    private final SchemaRefreshMode schemaRefreshMode;
    private final boolean flushLsnOnSource;

    public PostgresConnectorConfig(Configuration config) {
        super(
                config,
                new SystemTablesPredicate(),
                x -> x.schema() + "." + x.table(),
                DEFAULT_SNAPSHOT_FETCH_SIZE,
                ColumnFilterMode.SCHEMA,
                false);

        this.logicalDecodingMessageFilter = new LogicalDecodingMessageFilter(config.getString(LOGICAL_DECODING_MESSAGE_PREFIX_INCLUDE_LIST),
                config.getString(LOGICAL_DECODING_MESSAGE_PREFIX_EXCLUDE_LIST));
        String hstoreHandlingModeStr = config.getString(PostgresConnectorConfig.HSTORE_HANDLING_MODE);
        this.hStoreHandlingMode = HStoreHandlingMode.parse(hstoreHandlingModeStr);
        this.intervalHandlingMode = IntervalHandlingMode.parse(config.getString(PostgresConnectorConfig.INTERVAL_HANDLING_MODE));
        this.snapshotMode = SnapshotMode.parse(config.getString(SNAPSHOT_MODE));
        this.schemaRefreshMode = SchemaRefreshMode.parse(config.getString(SCHEMA_REFRESH_MODE));
        this.flushLsnOnSource = config.getBoolean(SHOULD_FLUSH_LSN_IN_SOURCE_DB);
    }

    protected String hostname() {
        return getConfig().getString(HOSTNAME);
    }

    protected int port() {
        return getConfig().getInteger(PORT);
    }

    public String databaseName() {
        return getConfig().getString(DATABASE_NAME);
    }

    protected LogicalDecoder plugin() {
        return LogicalDecoder.parse(getConfig().getString(PLUGIN_NAME));
    }

    protected String slotName() {
        return getConfig().getString(SLOT_NAME);
    }

    protected boolean dropSlotOnStop() {
        if (getConfig().hasKey(DROP_SLOT_ON_STOP.name())) {
            return getConfig().getBoolean(DROP_SLOT_ON_STOP);
        }
        // Return default value
        return getConfig().getBoolean(DROP_SLOT_ON_STOP);
    }

    public String publicationName() {
        return getConfig().getString(PUBLICATION_NAME);
    }

    protected AutoCreateMode publicationAutocreateMode() {
        return AutoCreateMode.parse(getConfig().getString(PUBLICATION_AUTOCREATE_MODE));
    }

    protected String streamParams() {
        return getConfig().getString(STREAM_PARAMS);
    }

    public int maxRetries() {
        return getConfig().getInteger(MAX_RETRIES);
    }

    public Duration retryDelay() {
        return Duration.ofMillis(getConfig().getInteger(RETRY_DELAY_MS));
    }

    protected Duration statusUpdateInterval() {
        return Duration.ofMillis(getConfig().getLong(PostgresConnectorConfig.STATUS_UPDATE_INTERVAL_MS));
    }

    public LogicalDecodingMessageFilter getMessageFilter() {
        return logicalDecodingMessageFilter;
    }

    protected HStoreHandlingMode hStoreHandlingMode() {
        return hStoreHandlingMode;
    }

    protected IntervalHandlingMode intervalHandlingMode() {
        return intervalHandlingMode;
    }

    protected boolean includeUnknownDatatypes() {
        return getConfig().getBoolean(INCLUDE_UNKNOWN_DATATYPES);
    }

    public Map<String, ConfigValue> validate() {
        return getConfig().validate(ALL_FIELDS);
    }

    protected Snapshotter getSnapshotter() {
        return this.snapshotMode.getSnapshotter(getConfig());
    }

    protected boolean skipRefreshSchemaOnMissingToastableData() {
        return SchemaRefreshMode.COLUMNS_DIFF_EXCLUDE_UNCHANGED_TOAST == this.schemaRefreshMode;
    }

    protected Duration xminFetchInterval() {
        return Duration.ofMillis(getConfig().getLong(PostgresConnectorConfig.XMIN_FETCH_INTERVAL));
    }

    public boolean isFlushLsnOnSource() {
        return flushLsnOnSource;
    }

    @Override
    public byte[] getUnavailableValuePlaceholder() {
        String placeholder = getConfig().getString(UNAVAILABLE_VALUE_PLACEHOLDER);
        if (placeholder.startsWith("hex:")) {
            return Strings.hexStringToByteArray(placeholder.substring(4));
        }
        return placeholder.getBytes();
    }

    protected int moneyFractionDigits() {
        return getConfig().getInteger(MONEY_FRACTION_DIGITS);
    }

    @Override
    protected SourceInfoStructMaker<? extends AbstractSourceInfo> getSourceInfoStructMaker(Version version) {
        return new PostgresSourceInfoStructMaker(Module.name(), Module.version(), this);
    }

    private static final ConfigDefinition CONFIG_DEFINITION = RelationalDatabaseConnectorConfig.CONFIG_DEFINITION.edit()
            .name("Postgres")
            .excluding(
                    CommonConnectorConfig.SKIPPED_OPERATIONS,
                    RelationalDatabaseConnectorConfig.INCREMENTAL_SNAPSHOT_ISOLATION_MODE)
            .type(
                    HOSTNAME,
                    PORT,
                    USER,
                    PASSWORD,
                    DATABASE_NAME,
                    PLUGIN_NAME,
                    SLOT_NAME,
                    PUBLICATION_NAME,
                    PUBLICATION_AUTOCREATE_MODE,
                    DROP_SLOT_ON_STOP,
                    STREAM_PARAMS,
                    ON_CONNECT_STATEMENTS,
                    SSL_MODE,
                    SSL_CLIENT_CERT,
                    SSL_CLIENT_KEY_PASSWORD,
                    SSL_ROOT_CERT,
                    SSL_CLIENT_KEY,
                    MAX_RETRIES,
                    RETRY_DELAY_MS,
                    SSL_SOCKET_FACTORY,
                    STATUS_UPDATE_INTERVAL_MS,
                    TCP_KEEPALIVE,
                    XMIN_FETCH_INTERVAL,
                    // Use this connector's implementation rather than common connector's flavor
                    SKIPPED_OPERATIONS,
                    SHOULD_FLUSH_LSN_IN_SOURCE_DB)
            .events(INCLUDE_UNKNOWN_DATATYPES)
            .connector(
                    SNAPSHOT_MODE,
                    SNAPSHOT_MODE_CLASS,
                    INCREMENTAL_SNAPSHOT_ISOLATION_MODE,
                    HSTORE_HANDLING_MODE,
                    BINARY_HANDLING_MODE,
                    SCHEMA_NAME_ADJUSTMENT_MODE,
                    INTERVAL_HANDLING_MODE,
                    SCHEMA_REFRESH_MODE,
                    INCREMENTAL_SNAPSHOT_CHUNK_SIZE,
                    UNAVAILABLE_VALUE_PLACEHOLDER,
                    LOGICAL_DECODING_MESSAGE_PREFIX_INCLUDE_LIST,
                    LOGICAL_DECODING_MESSAGE_PREFIX_EXCLUDE_LIST)
            .excluding(INCLUDE_SCHEMA_CHANGES)
            .create();

    /**
     * The set of {@link Field}s defined as part of this configuration.
     */
    public static Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    public static ConfigDef configDef() {
        return CONFIG_DEFINITION.configDef();
    }

    // Source of the validation rules - https://doxygen.postgresql.org/slot_8c.html#afac399f07320b9adfd2c599cf822aaa3
    private static int validateReplicationSlotName(Configuration config, Field field, Field.ValidationOutput problems) {
        final String name = config.getString(field);
        int errors = 0;
        if (name != null) {
            if (!name.matches("[a-z0-9_]{1,63}")) {
                problems.accept(field, name, "Valid replication slot name must contain only digits, lowercase characters and underscores with length <= 63");
                ++errors;
            }
        }
        return errors;
    }

    private static int validateLogicalDecodingMessageExcludeList(Configuration config, Field field, Field.ValidationOutput problems) {
        String includeList = config.getString(LOGICAL_DECODING_MESSAGE_PREFIX_INCLUDE_LIST);
        String excludeList = config.getString(LOGICAL_DECODING_MESSAGE_PREFIX_EXCLUDE_LIST);

        if (includeList != null && excludeList != null) {
            problems.accept(LOGICAL_DECODING_MESSAGE_PREFIX_EXCLUDE_LIST, excludeList,
                    "\"logical_decoding_message.prefix.include.list\" is already specified");
            return 1;
        }
        return 0;
    }

    private static int validateFlushLsnSource(Configuration config, Field field, Field.ValidationOutput problems) {
        if (config.getString(PostgresConnectorConfig.SHOULD_FLUSH_LSN_IN_SOURCE_DB, "true").equalsIgnoreCase("false")) {
            LOGGER.warn("Property '" + PostgresConnectorConfig.SHOULD_FLUSH_LSN_IN_SOURCE_DB.name()
                    + "' is set to 'false', the LSN will not be flushed to the database source and WAL logs will not be cleared. User is expected to handle this outside Debezium.");
        }
        return 0;
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
    public int getDefaultTransactionIsolationLevel() {
        SnapshotIsolationMode defaultSnapshotIsolationLevel = SnapshotIsolationMode.parse(INCREMENTAL_SNAPSHOT_ISOLATION_MODE.defaultValueAsString());
        return super.getTransactionIsolationLevel(defaultSnapshotIsolationLevel);
    }

    private static class SystemTablesPredicate implements TableFilter {
        protected static final List<String> SYSTEM_SCHEMAS = Arrays.asList("pg_catalog", "information_schema");
        // these are tables that may be placed in the user's schema but are system tables. This typically includes modules
        // that install system tables such as the GEO module
        protected static final List<String> SYSTEM_TABLES = Arrays.asList("spatial_ref_sys");
        protected static final String TEMP_TABLE_SCHEMA_PREFIX = "pg_temp";

        @Override
        public boolean isIncluded(TableId t) {
            return t.schema() != null && !SYSTEM_SCHEMAS.contains(t.schema().toLowerCase()) &&
                    t.table() != null && !SYSTEM_TABLES.contains(t.table().toLowerCase()) &&
                    !t.schema().startsWith(TEMP_TABLE_SCHEMA_PREFIX);
        }
    }
}
