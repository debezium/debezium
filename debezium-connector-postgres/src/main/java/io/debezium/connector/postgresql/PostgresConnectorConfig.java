/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.connector.postgresql.connection.MessageDecoder;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.connection.pgproto.PgProtoMessageDecoder;
import io.debezium.connector.postgresql.connection.wal2json.NonStreamingWal2JsonMessageDecoder;
import io.debezium.connector.postgresql.connection.wal2json.StreamingWal2JsonMessageDecoder;
import io.debezium.connector.postgresql.snapshot.AlwaysSnapshotter;
import io.debezium.connector.postgresql.snapshot.InitialOnlySnapshotter;
import io.debezium.connector.postgresql.snapshot.InitialSnapshotter;
import io.debezium.connector.postgresql.snapshot.NeverSnapshotter;
import io.debezium.connector.postgresql.spi.Snapshotter;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;

/**
 * The configuration properties for the {@link PostgresConnector}
 *
 * @author Horia Chiorean
 */
public class PostgresConnectorConfig extends RelationalDatabaseConnectorConfig {

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

        HStoreHandlingMode(String value){
            this.value=value;
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
        public static HStoreHandlingMode parse(String value){
            if (value == null) {
                return null ;
            }
            value = value.trim();
            for (HStoreHandlingMode option: HStoreHandlingMode.values()) {
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
        public static HStoreHandlingMode parse(String value, String defaultValue){
            HStoreHandlingMode mode = parse(value);
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
            for(SnapshotMode option: SnapshotMode.values()) {
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

    /**
     * The set of configuration options for how events are placed on Kafka topics
     */
    public enum TopicSelectionStrategy implements EnumeratedValue {
        /**
         * Create a topic for each distinct DB table
         */
        TOPIC_PER_TABLE("topic_per_table") {

            @Override
            public String getTopicName(TableId tableId, String prefix, String delimiter) {
                return String.join(delimiter, prefix, tableId.schema(), tableId.table());
            }
        },

        /**
         * Create a topic for an entire DB schema
         */
        TOPIC_PER_SCHEMA("topic_per_schema") {

            @Override
            public String getTopicName(TableId tableId, String prefix, String delimiter) {
                return String.join(delimiter, prefix, tableId.schema());
            }
        };

        private String value;

        @Override
        public String getValue() {
            return value;
        }

        public abstract String getTopicName(TableId tableId, String prefix, String delimiter);

        TopicSelectionStrategy(String value) {
            this.value = value;
        }
        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static TopicSelectionStrategy parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (TopicSelectionStrategy option : TopicSelectionStrategy.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }
    }

    public enum LogicalDecoder implements EnumeratedValue {
        DECODERBUFS("decoderbufs") {
            @Override
            public MessageDecoder messageDecoder() {
                return new PgProtoMessageDecoder();
            }

            @Override
            public String getPostgresPluginName() {
                return getValue();
            }
        },
        WAL2JSON_STREAMING("wal2json_streaming") {
            @Override
            public MessageDecoder messageDecoder() {
                return new StreamingWal2JsonMessageDecoder();
            }

            @Override
            public String getPostgresPluginName() {
                return "wal2json";
            }
        },
        WAL2JSON_RDS_STREAMING("wal2json_rds_streaming") {
            @Override
            public MessageDecoder messageDecoder() {
                return new StreamingWal2JsonMessageDecoder();
            }

            @Override
            public boolean forceRds() {
                return true;
            }

            @Override
            public String getPostgresPluginName() {
                return "wal2json";
            }
        },
        WAL2JSON("wal2json") {
            @Override
            public MessageDecoder messageDecoder() {
                return new NonStreamingWal2JsonMessageDecoder();
            }

            @Override
            public String getPostgresPluginName() {
                return "wal2json";
            }
        },
        WAL2JSON_RDS("wal2json_rds") {
            @Override
            public MessageDecoder messageDecoder() {
                return new NonStreamingWal2JsonMessageDecoder();
            }

            @Override
            public boolean forceRds() {
                return true;
            }

            @Override
            public String getPostgresPluginName() {
                return "wal2json";
            }
        };

        private final String decoderName;

        LogicalDecoder(String decoderName) {
            this.decoderName = decoderName;
        }

        public abstract MessageDecoder messageDecoder();

        public boolean forceRds() {
            return false;
        }

        public static LogicalDecoder parse(String s) {
            return valueOf(s.trim().toUpperCase());
        }

        @Override
        public String getValue() {
            return decoderName;
        }

        public abstract String getPostgresPluginName();
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

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresConnectorConfig.class);

    protected static final String DATABASE_CONFIG_PREFIX = "database.";
    protected static final int DEFAULT_PORT = 5_432;
    protected static final int DEFAULT_SNAPSHOT_FETCH_SIZE = 10_240;
    protected static final long DEFAULT_SNAPSHOT_LOCK_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(10);

    private static final String TABLE_WHITELIST_NAME = "table.whitelist";

    public static final Field PLUGIN_NAME = Field.create("plugin.name")
                                              .withDisplayName("Plugin")
                                              .withEnum(LogicalDecoder.class, LogicalDecoder.DECODERBUFS)
                                              .withWidth(Width.MEDIUM)
                                              .withImportance(Importance.MEDIUM)
                                              .withDescription("The name of the Postgres logical decoding plugin installed on the server. " +
                                                      "Supported values are '"+ LogicalDecoder.DECODERBUFS.getValue() + "' and '"+ LogicalDecoder.WAL2JSON.getValue() + "'. " +
                                                      "Defaults to '"+ LogicalDecoder.DECODERBUFS.getValue() + "'.");

    public static final Field SLOT_NAME = Field.create("slot.name")
                                              .withDisplayName("Slot")
                                              .withType(Type.STRING)
                                              .withWidth(Width.MEDIUM)
                                              .withImportance(Importance.MEDIUM)
                                              .withDefault(ReplicationConnection.Builder.DEFAULT_SLOT_NAME)
                                              .withDescription("The name of the Postgres logical decoding slot created for streaming changes from a plugin." +
                                                               "Defaults to 'debezium");

    public static final Field DROP_SLOT_ON_STOP = Field.create("slot.drop_on_stop")
                                                        .withDisplayName("Drop slot on stop")
                                                        .withType(Type.BOOLEAN)
                                                        .withDefault(false)
                                                        .withImportance(Importance.MEDIUM)
                                                        .withDescription(
                                                                "Whether or not to drop the logical replication slot when the connector finishes orderly" +
                                                                "By default the replication is kept so that on restart progress can resume from the last recorded location");

    public static final Field STREAM_PARAMS = Field.create("slot.stream.params")
                                                        .withDisplayName("Optional parameters to pass to the logical decoder when the stream is started.")
                                                        .withType(Type.STRING)
                                                        .withWidth(Width.LONG)
                                                        .withImportance(Importance.LOW)
                                                        .withDescription(
                                                                "Any optional parameters used by logical decoding plugin. Semi-colon separated. E.g. 'add-tables=public.table,public.table2;include-lsn=true'");

    public static final Field HOSTNAME = Field.create(DATABASE_CONFIG_PREFIX + JdbcConfiguration.HOSTNAME)
                                              .withDisplayName("Hostname")
                                              .withType(Type.STRING)
                                              .withWidth(Width.MEDIUM)
                                              .withImportance(Importance.HIGH)
                                              .withValidation(Field::isRequired)
                                              .withDescription("Resolvable hostname or IP address of the Postgres database server.");

    public static final Field PORT = Field.create(DATABASE_CONFIG_PREFIX + JdbcConfiguration.PORT)
                                          .withDisplayName("Port")
                                          .withType(Type.INT)
                                          .withWidth(Width.SHORT)
                                          .withDefault(DEFAULT_PORT)
                                          .withImportance(Importance.HIGH)
                                          .withValidation(Field::isInteger)
                                          .withDescription("Port of the Postgres database server.");

    public static final Field USER = Field.create(DATABASE_CONFIG_PREFIX + JdbcConfiguration.USER)
                                          .withDisplayName("User")
                                          .withType(Type.STRING)
                                          .withWidth(Width.SHORT)
                                          .withImportance(Importance.HIGH)
                                          .withValidation(Field::isRequired)
                                          .withDescription("Name of the Postgres database user to be used when connecting to the database.");

    public static final Field PASSWORD = Field.create(DATABASE_CONFIG_PREFIX + JdbcConfiguration.PASSWORD)
                                              .withDisplayName("Password")
                                              .withType(Type.PASSWORD)
                                              .withWidth(Width.SHORT)
                                              .withImportance(Importance.HIGH)
                                              .withDescription("Password of the Postgres database user to be used when connecting to the database.");

    public static final Field DATABASE_NAME = Field.create(DATABASE_CONFIG_PREFIX + JdbcConfiguration.DATABASE)
                                                   .withDisplayName("Database")
                                                   .withType(Type.STRING)
                                                   .withWidth(Width.MEDIUM)
                                                   .withImportance(Importance.HIGH)
                                                   .withValidation(Field::isRequired)
                                                   .withDescription("The name of the database the connector should be monitoring");

    public static final Field ON_CONNECT_STATEMENTS = Field.create(DATABASE_CONFIG_PREFIX + JdbcConfiguration.ON_CONNECT_STATEMENTS)
                                                           .withDisplayName("Initial statements")
                                                           .withType(Type.STRING)
                                                           .withWidth(Width.LONG)
                                                           .withImportance(Importance.LOW)
                                                           .withDescription("A semicolon separated list of SQL statements to be executed when a JDBC connection to the database is established. "
                                                                   + "Note that the connector may establish JDBC connections at its own discretion, so this should typically be used for configuration"
                                                                   + "of session parameters only, but not for executing DML statements. Use doubled semicolon (';;') to use a semicolon as a character "
                                                                   + "and not as a delimiter.");

    public static final Field TOPIC_SELECTION_STRATEGY = Field.create("topic.selection.strategy")
                                                              .withDisplayName("Topic selection strategy")
                                                              .withEnum(TopicSelectionStrategy.class, TopicSelectionStrategy.TOPIC_PER_TABLE)
                                                              .withWidth(Width.MEDIUM)
                                                              .withImportance(Importance.LOW)
                                                              .withDescription("How events received from the DB should be placed on topics. Options include"
                                                      + "'table' (the default) each DB table will have a separate Kafka topic; "
                                                      + "'schema' there will be one Kafka topic per DB schema; events from multiple topics belonging to the same schema will be placed on the same topic");

    public static final Field SSL_MODE = Field.create(DATABASE_CONFIG_PREFIX + "sslmode")
                                              .withDisplayName("SSL mode")
                                              .withEnum(SecureConnectionMode.class, SecureConnectionMode.DISABLED)
                                              .withWidth(Width.MEDIUM)
                                              .withImportance(Importance.MEDIUM)
                                              .withDescription("Whether to use an encrypted connection to Postgres. Options include"
                                                      + "'disable' (the default) to use an unencrypted connection; "
                                                      + "'require' to use a secure (encrypted) connection, and fail if one cannot be established; "
                                                      + "'verify-ca' like 'required' but additionally verify the server TLS certificate against the configured Certificate Authority "
                                                      + "(CA) certificates, or fail if no valid matching CA certificates are found; or"
                                                      + "'verify-full' like 'verify-ca' but additionally verify that the server certificate matches the host to which the connection is attempted.");

    public static final Field SSL_CLIENT_CERT = Field.create(DATABASE_CONFIG_PREFIX + "sslcert")
                                                     .withDisplayName("SSL Client Certificate")
                                                     .withType(Type.STRING)
                                                     .withWidth(Width.LONG)
                                                     .withImportance(Importance.MEDIUM)
                                                     .withDescription("File containing the SSL Certificate for the client. See the Postgres SSL docs for further information");

    public static final Field SSL_CLIENT_KEY = Field.create(DATABASE_CONFIG_PREFIX + "sslkey")
                                                    .withDisplayName("SSL Client Key")
                                                    .withType(Type.STRING)
                                                    .withWidth(Width.LONG)
                                                    .withImportance(Importance.MEDIUM)
                                                    .withDescription("File containing the SSL private key for the client. See the Postgres SSL docs for further information");

    public static final Field SSL_CLIENT_KEY_PASSWORD = Field.create(DATABASE_CONFIG_PREFIX + "sslpassword")
                                                             .withDisplayName("SSL Client Key Password")
                                                             .withType(Type.PASSWORD)
                                                             .withWidth(Width.MEDIUM)
                                                             .withImportance(Importance.MEDIUM)
                                                             .withDescription("Password to access the client private key from the file specified by 'database.sslkey'. See the Postgres SSL docs for further information");

    public static final Field SSL_ROOT_CERT = Field.create(DATABASE_CONFIG_PREFIX + "sslrootcert")
                                                   .withDisplayName("SSL Root Certificate")
                                                   .withType(Type.STRING)
                                                   .withWidth(Width.LONG)
                                                   .withImportance(Importance.MEDIUM)
                                                   .withDescription("File containing the root certificate(s) against which the server is validated. See the Postgres JDBC SSL docs for further information");

    public static final Field SSL_SOCKET_FACTORY = Field.create(DATABASE_CONFIG_PREFIX + "sslfactory")
                                                        .withDisplayName("SSL Root Certificate")
                                                        .withType(Type.STRING)
                                                        .withWidth(Width.LONG)
                                                        .withImportance(Importance.MEDIUM)
                                                        .withDescription("A name of class to that creates SSL Sockets. Use org.postgresql.ssl.NonValidatingFactory to disable SSL validation in development environments");

    /**
     * A comma-separated list of regular expressions that match schema names to be monitored.
     * May not be used with {@link #SCHEMA_BLACKLIST}.
     */
    public static final Field SCHEMA_WHITELIST = Field.create("schema.whitelist")
                                                      .withDisplayName("Schemas")
                                                      .withType(Type.LIST)
                                                      .withWidth(Width.LONG)
                                                      .withImportance(Importance.HIGH)
                                                      .withDependents(TABLE_WHITELIST_NAME)
                                                      .withDescription("The schemas for which events should be captured");

    /**
     * A comma-separated list of regular expressions that match schema names to be excluded from monitoring.
     * May not be used with {@link #SCHEMA_WHITELIST}.
     */
    public static final Field SCHEMA_BLACKLIST = Field.create("schema.blacklist")
                                                      .withDisplayName("Exclude Schemas")
                                                      .withType(Type.STRING)
                                                      .withWidth(Width.LONG)
                                                      .withImportance(Importance.MEDIUM)
                                                      .withValidation(PostgresConnectorConfig::validateSchemaBlacklist)
                                                      .withInvisibleRecommender()
                                                      .withDescription("");


    /**
     * A comma-separated list of regular expressions that match the fully-qualified names of tables to be monitored.
     * Fully-qualified names for tables are of the form {@code <schemaName>.<tableName>} or
     * {@code <databaseName>.<schemaName>.<tableName>}. May not be used with {@link #TABLE_BLACKLIST}, and superseded by schema
     * inclusions/exclusions.
     */
    public static final Field TABLE_WHITELIST = Field.create(TABLE_WHITELIST_NAME)
                                                     .withDisplayName("Tables")
                                                     .withType(Type.LIST)
                                                     .withWidth(Width.LONG)
                                                     .withImportance(Importance.HIGH)
                                                     .withValidation(Field::isListOfRegex)
                                                     .withDescription("The tables for which changes are to be captured");

    /**
     * A comma-separated list of regular expressions that match the fully-qualified names of tables to be excluded from
     * monitoring. Fully-qualified names for tables are of the form {@code <schemaName>.<tableName>} or
     * {@code <databaseName>.<schemaName>.<tableName>}. May not be used with {@link #TABLE_WHITELIST}.
     */
    public static final Field TABLE_BLACKLIST = Field.create("table.blacklist")
                                                     .withDisplayName("Exclude Tables")
                                                     .withType(Type.STRING)
                                                     .withWidth(Width.LONG)
                                                     .withImportance(Importance.MEDIUM)
                                                     .withValidation(Field::isListOfRegex, PostgresConnectorConfig::validateTableBlacklist)
                                                     .withInvisibleRecommender();

    //TODO author=Horia Chiorean date=25/10/2016 description=PG 9.x logical decoding does not support schema changes
    public static final Field INCLUDE_SCHEMA_CHANGES = Field.create("include.schema.changes")
                                                            .withDisplayName("Include database schema changes")
                                                            .withType(Type.BOOLEAN)
                                                            .withWidth(Width.SHORT)
                                                            .withImportance(Importance.MEDIUM)
                                                            .withDescription("Whether the connector should publish changes in the database schema to a Kafka topic with "
                                                                    + "the same name as the database server name."
                                                                    + "The default is 'false' because atm this feature is not supported by Postgres logical decoding")
                                                            .withDefault(false);

    public static final Field SNAPSHOT_MODE = Field.create("snapshot.mode")
                                                   .withDisplayName("Snapshot mode")
                                                   .withEnum(SnapshotMode.class, SnapshotMode.INITIAL)
                                                   .withWidth(Width.SHORT)
                                                   .withImportance(Importance.MEDIUM)
                                                   .withDescription("The criteria for running a snapshot upon startup of the connector. "
                                                           + "Options include: "
                                                           + "'always' to specify that the connector run a snapshot each time it starts up; "
                                                           + "'initial' (the default) to specify the connector can run a snapshot only when no offsets are available for the logical server name; "
                                                           + "'initial_only' same as 'initial' except the connector should stop after completing the snapshot and before it would normally start emitting changes;"
                                                           + "'never' to specify the connector should never run a snapshot and that upon first startup the connector should read from the last position (LSN) recorded by the server; and"
                                                           + "'custom' to specify a custom class with 'snapshot.custom_class' which will be loaded and used to determine the snapshot, see docs for more details.");

    public static final Field SNAPSHOT_MODE_CLASS = Field.create("snapshot.custom.class")
                                                         .withDisplayName("Snapshot Mode Custom Class")
                                                         .withType(Type.STRING)
                                                         .withWidth(Width.MEDIUM)
                                                         .withImportance(Importance.MEDIUM)
                                                         .withValidation((config, field, output) -> {
                                                             if (config.getString(SNAPSHOT_MODE).toLowerCase().equals("custom") && config.getString(field).isEmpty()) {
                                                                output.accept(field, "", "snapshot.custom_class cannot be empty when snapshot.mode 'custom' is defined");
                                                                return 1;
                                                             }
                                                             return 0;
                                                         })
                                                         .withDescription("When 'snapshot.mode' is set as custom, this setting must be set to specify a fully qualified class name to load (via the default class loader)."
                                                                         + "This class must implement the 'Snapshotter' interface and is called on each app boot to determine whether to do a snapshot and how to build queries.");

    public static final Field SNAPSHOT_LOCK_TIMEOUT_MS = Field.create("snapshot.lock.timeout.ms")
                                                              .withDisplayName("Snapshot lock timeout (ms)")
                                                              .withWidth(Width.LONG)
                                                              .withType(Type.LONG)
                                                              .withImportance(Importance.MEDIUM)
                                                              .withDefault(DEFAULT_SNAPSHOT_LOCK_TIMEOUT_MILLIS)
                                                              .withDescription("The maximum number of millis to wait for table locks at the beginning of a snapshot. If locks cannot be acquired in this " +
                                                                               "time frame, the snapshot will be aborted. Defaults to 10 seconds");

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

    public static final Field HSTORE_HANDLING_MODE = Field.create("hstore.handling.mode")
                                                          .withDisplayName("HStore Handling")
                                                          .withEnum(HStoreHandlingMode.class, HStoreHandlingMode.JSON)
                                                          .withWidth(Width.MEDIUM)
                                                          .withImportance(Importance.LOW)
                                                          .withDescription("Specify how HSTORE columns should be represented in change events, including:"
                                                                + "'json' represents values as json string"
                                                                + "'map' (default) represents values using java.util.Map");

    public static final Field STATUS_UPDATE_INTERVAL_MS = Field.create("status.update.interval.ms")
                                                          .withDisplayName("Status update interval (ms)")
                                                          .withType(Type.INT) // Postgres doesn't accept long for this value
                                                          .withDefault(10_000)
                                                          .withWidth(Width.SHORT)
                                                          .withImportance(Importance.MEDIUM)
                                                          .withDescription("Frequency in milliseconds for sending replication connection status updates to the server. Defaults to 10 seconds (10000 ms).")
                                                          .withValidation(Field::isPositiveInteger);

    public static final Field TCP_KEEPALIVE = Field.create(DATABASE_CONFIG_PREFIX + "tcpKeepAlive")
                                                            .withDisplayName("TCP keep-alive probe")
                                                            .withType(Type.BOOLEAN)
                                                            .withDefault(true)
                                                            .withWidth(Width.SHORT)
                                                            .withImportance(Importance.MEDIUM)
                                                            .withDescription("Enable or disable TCP keep-alive probe to avoid dropping TCP connection")
                                                            .withValidation(Field::isBoolean);

    public static final Field INCLUDE_UNKNOWN_DATATYPES = Field.create("include.unknown.datatypes")
                                                            .withDisplayName("Include unknown datatypes")
                                                            .withType(Type.BOOLEAN)
                                                            .withDefault(false)
                                                            .withWidth(Width.SHORT)
                                                            .withImportance(Importance.MEDIUM)
                                                            .withDescription("Specify whether the fields of data type not supported by Debezium should be processed:"
                                                                    + "'false' (the default) omits the fields; "
                                                                    + "'true' converts the field into an implementation dependent binary representation.");

    public static final Field SCHEMA_REFRESH_MODE = Field.create("schema.refresh.mode")
            .withDisplayName("Schema refresh mode")
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
            .withWidth(Width.SHORT)
            .withDefault(0L)
            .withImportance(Importance.MEDIUM)
            .withDescription("Specify how often (in ms) the xmin will be fetched from the replication slot. " +
                    "This xmin value is exposed by the slot which gives a lower bound of where a new replication slot could start from. " +
                    "The lower the value, the more likely this value is to be the current 'true' value, but the bigger the performance cost. " +
                    "The bigger the value, the less likely this value is to be the current 'true' value, but the lower the performance penalty. " +
                    "The default is set to 0 ms, which disables tracking xmin.")
            .withValidation(Field::isNonNegativeLong);
    /**
     * The set of {@link Field}s defined as part of this configuration.
     */
    public static Field.Set ALL_FIELDS = Field.setOf(PLUGIN_NAME, SLOT_NAME, DROP_SLOT_ON_STOP, STREAM_PARAMS,
                                                     DATABASE_NAME, USER, PASSWORD, HOSTNAME, PORT, ON_CONNECT_STATEMENTS, RelationalDatabaseConnectorConfig.SERVER_NAME,
                                                     TOPIC_SELECTION_STRATEGY, CommonConnectorConfig.MAX_BATCH_SIZE,
                                                     CommonConnectorConfig.MAX_QUEUE_SIZE, CommonConnectorConfig.POLL_INTERVAL_MS,
                                                     CommonConnectorConfig.SNAPSHOT_DELAY_MS, CommonConnectorConfig.SNAPSHOT_FETCH_SIZE,
                                                     Heartbeat.HEARTBEAT_INTERVAL,
                                                     Heartbeat.HEARTBEAT_TOPICS_PREFIX,
                                                     SCHEMA_WHITELIST,
                                                     SCHEMA_BLACKLIST, TABLE_WHITELIST, TABLE_BLACKLIST,
                                                     COLUMN_BLACKLIST, SNAPSHOT_MODE, TIME_PRECISION_MODE, DECIMAL_HANDLING_MODE, HSTORE_HANDLING_MODE,
                                                     SSL_MODE, SSL_CLIENT_CERT, SSL_CLIENT_KEY_PASSWORD,
                                                     SSL_ROOT_CERT, SSL_CLIENT_KEY, SNAPSHOT_LOCK_TIMEOUT_MS, SSL_SOCKET_FACTORY,
                                                     STATUS_UPDATE_INTERVAL_MS, TCP_KEEPALIVE, INCLUDE_UNKNOWN_DATATYPES,
                                                     RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE, SCHEMA_REFRESH_MODE, CommonConnectorConfig.TOMBSTONES_ON_DELETE,
                                                     XMIN_FETCH_INTERVAL, SNAPSHOT_MODE_CLASS, CommonConnectorConfig.SOURCE_STRUCT_MAKER_VERSION);

    private final TemporalPrecisionMode temporalPrecisionMode;
    private final HStoreHandlingMode  hStoreHandlingMode;
    private final SnapshotMode snapshotMode;
    private final SchemaRefreshMode schemaRefreshMode;


    protected PostgresConnectorConfig(Configuration config) {
        super(
                config,
                config.getString(RelationalDatabaseConnectorConfig.SERVER_NAME),
                null, // TODO whitelist handling implemented locally here for the time being
                null,
                DEFAULT_SNAPSHOT_FETCH_SIZE
        );

        this.temporalPrecisionMode = TemporalPrecisionMode.parse(config.getString(TIME_PRECISION_MODE));
        String hstoreHandlingModeStr = config.getString(PostgresConnectorConfig.HSTORE_HANDLING_MODE);
        HStoreHandlingMode hStoreHandlingMode = HStoreHandlingMode.parse(hstoreHandlingModeStr);
        this.hStoreHandlingMode = hStoreHandlingMode;
        this.snapshotMode = SnapshotMode.parse(config.getString(SNAPSHOT_MODE));
        this.schemaRefreshMode = SchemaRefreshMode.parse(config.getString(SCHEMA_REFRESH_MODE));
    }

    protected String hostname() {
        return getConfig().getString(HOSTNAME);
    }

    protected int port() {
        return getConfig().getInteger(PORT);
    }

    protected String databaseName() {
        return getConfig().getString(DATABASE_NAME);
    }

    protected LogicalDecoder plugin() {
        return LogicalDecoder.parse(getConfig().getString(PLUGIN_NAME));
    }

    protected String slotName() {
        return getConfig().getString(SLOT_NAME);
    }

    protected boolean dropSlotOnStop() {
        return getConfig().getBoolean(DROP_SLOT_ON_STOP);
    }

    protected String streamParams() {
        return getConfig().getString(STREAM_PARAMS);
    }

    protected Duration statusUpdateInterval() {
        return Duration.ofMillis(getConfig().getLong(PostgresConnectorConfig.STATUS_UPDATE_INTERVAL_MS));
    }

    protected TemporalPrecisionMode temporalPrecisionMode() {
        return temporalPrecisionMode;
    }

    protected HStoreHandlingMode hStoreHandlingMode() {
        return hStoreHandlingMode;
    }

    protected boolean includeUnknownDatatypes() {
        return getConfig().getBoolean(INCLUDE_UNKNOWN_DATATYPES);
    }

    public Configuration jdbcConfig() {
        return getConfig().subset(DATABASE_CONFIG_PREFIX, true);
    }

    protected TopicSelectionStrategy topicSelectionStrategy() {
        //TODO author=Horia Chiorean date=04/11/2016 description=implement this fully once the changes for MySQL are merged
        //String value = config.getString(PostgresConnectorConfig.TOPIC_SELECTION_STRATEGY);
        //return PostgresConnectorConfig.TopicSelectionStrategy.parse(value);
        return PostgresConnectorConfig.TopicSelectionStrategy.TOPIC_PER_TABLE;
    }

    protected Map<String, ConfigValue> validate() {
        return getConfig().validate(ALL_FIELDS);
    }

    protected String schemaBlacklist() {
        return getConfig().getString(SCHEMA_BLACKLIST);
    }

    protected String schemaWhitelist() {
        return getConfig().getString(SCHEMA_WHITELIST);
    }

    protected String tableBlacklist() {
        return getConfig().getString(TABLE_BLACKLIST);
    }

    protected String tableWhitelist() {
        return getConfig().getString(TABLE_WHITELIST);
    }

    protected String columnBlacklist() {
        return getConfig().getString(COLUMN_BLACKLIST);
    }

    protected long snapshotLockTimeoutMillis() {
        return getConfig().getLong(PostgresConnectorConfig.SNAPSHOT_LOCK_TIMEOUT_MS);
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

    @Override
    protected SourceInfoStructMaker<? extends AbstractSourceInfo> getSourceInfoStructMaker(Version version) {
        switch (version) {
        case V1:
            return new LegacyV1PostgresSourceInfoStructMaker(Module.name(), Module.version(), this);
        default:
            return new PostgresSourceInfoStructMaker(Module.name(), Module.version(), this);
        }
    }

    protected static ConfigDef configDef() {
        ConfigDef config = new ConfigDef();
        Field.group(config, "Postgres", SLOT_NAME, PLUGIN_NAME, RelationalDatabaseConnectorConfig.SERVER_NAME, DATABASE_NAME, HOSTNAME, PORT,
                    USER, PASSWORD, ON_CONNECT_STATEMENTS, SSL_MODE, SSL_CLIENT_CERT, SSL_CLIENT_KEY_PASSWORD, SSL_ROOT_CERT, SSL_CLIENT_KEY,
                    DROP_SLOT_ON_STOP, STREAM_PARAMS, SSL_SOCKET_FACTORY, STATUS_UPDATE_INTERVAL_MS, TCP_KEEPALIVE, XMIN_FETCH_INTERVAL, SNAPSHOT_MODE_CLASS);
        Field.group(config, "Events", SCHEMA_WHITELIST, SCHEMA_BLACKLIST, TABLE_WHITELIST, TABLE_BLACKLIST,
                    COLUMN_BLACKLIST, INCLUDE_UNKNOWN_DATATYPES, SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE,
                    CommonConnectorConfig.TOMBSTONES_ON_DELETE, Heartbeat.HEARTBEAT_INTERVAL,
                    Heartbeat.HEARTBEAT_TOPICS_PREFIX, CommonConnectorConfig.SOURCE_STRUCT_MAKER_VERSION);
        Field.group(config, "Connector", TOPIC_SELECTION_STRATEGY, CommonConnectorConfig.POLL_INTERVAL_MS, CommonConnectorConfig.MAX_BATCH_SIZE, CommonConnectorConfig.MAX_QUEUE_SIZE,
                    CommonConnectorConfig.SNAPSHOT_DELAY_MS, CommonConnectorConfig.SNAPSHOT_FETCH_SIZE,
                    SNAPSHOT_MODE, SNAPSHOT_LOCK_TIMEOUT_MS, TIME_PRECISION_MODE, DECIMAL_HANDLING_MODE, HSTORE_HANDLING_MODE, SCHEMA_REFRESH_MODE);

        return config;
    }

    private static int validateSchemaBlacklist(Configuration config, Field field, Field.ValidationOutput problems) {
        String whitelist = config.getString(SCHEMA_WHITELIST);
        String blacklist = config.getString(SCHEMA_BLACKLIST);
        if (whitelist != null && blacklist != null) {
            problems.accept(SCHEMA_BLACKLIST, blacklist, "Schema whitelist is already specified");
            return 1;
        }
        return 0;
    }

    private static int validateTableBlacklist(Configuration config, Field field, Field.ValidationOutput problems) {
        String whitelist = config.getString(TABLE_WHITELIST);
        String blacklist = config.getString(TABLE_BLACKLIST);
        if (whitelist != null && blacklist != null) {
            problems.accept(TABLE_BLACKLIST, blacklist, "Table whitelist is already specified");
            return 1;
        }
        return 0;
    }
}
