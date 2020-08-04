/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.config.Field.ValidationOutput;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;

/**
 * The configuration properties.
 */
public class MongoDbConnectorConfig extends CommonConnectorConfig {

    /**
     * The set of predefined SnapshotMode options or aliases.
     */
    public static enum SnapshotMode implements EnumeratedValue {

        /**
         * Always perform an initial snapshot when starting.
         */
        INITIAL("initial", true),

        /**
         * Never perform a snapshot and only receive new data changes.
         */
        NEVER("never", false);

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

    protected static final int DEFAULT_SNAPSHOT_FETCH_SIZE = 0;

    /**
     * The comma-separated list of hostname and port pairs (in the form 'host' or 'host:port') of the MongoDB servers in the
     * replica set.
     */
    public static final Field HOSTS = Field.create("mongodb.hosts")
            .withDisplayName("Hosts")
            .withType(Type.LIST)
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withValidation(MongoDbConnectorConfig::validateHosts)
            .withDescription("The hostname and port pairs (in the form 'host' or 'host:port') "
                    + "of the MongoDB server(s) in the replica set.");

    public static final Field LOGICAL_NAME = Field.create("mongodb.name")
            .withDisplayName("Namespace")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withValidation(Field::isRequired)
            .withDescription("Unique name that identifies the MongoDB replica set or cluster and all recorded offsets, and"
                    + "that is used as a prefix for all schemas and topics. "
                    + "Each distinct MongoDB installation should have a separate namespace and monitored by "
                    + "at most one Debezium connector.");

    public static final Field USER = Field.create("mongodb.user")
            .withDisplayName("User")
            .withType(Type.STRING)
            .withWidth(Width.SHORT)
            .withImportance(Importance.HIGH)
            .withDescription("Database user for connecting to MongoDB, if necessary.");

    public static final Field PASSWORD = Field.create("mongodb.password")
            .withDisplayName("Password")
            .withType(Type.PASSWORD)
            .withWidth(Width.SHORT)
            .withImportance(Importance.HIGH)
            .withDescription("Password to be used when connecting to MongoDB, if necessary.");

    public static final Field AUTH_SOURCE = Field.create("mongodb.authsource")
            .withDisplayName("Credentials Database")
            .withType(Type.STRING)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(ReplicaSetDiscovery.ADMIN_DATABASE_NAME)
            .withDescription("Database containing user credentials.");

    @Deprecated
    public static final Field POLL_INTERVAL_SEC = Field.create("mongodb.poll.interval.sec")
            .withDisplayName("Replica membership poll interval (sec)")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(30)
            .withValidation(Field::isPositiveInteger)
            .withDescription("(Deprecated, use mongodb.poll.interval.ms) Frequency in seconds to look for new, removed, " +
                    "or changed replica sets. Defaults to 30 seconds.");

    public static final Field MONGODB_POLL_INTERVAL_MS = Field.create("mongodb.poll.interval.ms")
            .withDisplayName("Replica membership poll interval (ms)")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(30000L)
            .withValidation(Field::isPositiveInteger)
            .withDescription("Frequency in milliseconds to look for new, removed, or changed replica sets.  Defaults to 30000 milliseconds.");

    public static final Field SSL_ENABLED = Field.create("mongodb.ssl.enabled")
            .withDisplayName("Enable SSL connection to MongoDB")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(false)
            .withValidation(Field::isBoolean)
            .withDescription("Should connector use SSL to connect to MongoDB instances");

    public static final Field SSL_ALLOW_INVALID_HOSTNAMES = Field.create("mongodb.ssl.invalid.hostname.allowed")
            .withDisplayName("Allow invalid hostnames for SSL connection")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(false)
            .withValidation(Field::isBoolean)
            .withDescription("Whether invalid host names are allowed when using SSL. If true the connection will not prevent man-in-the-middle attacks");

    public static final Field MAX_COPY_THREADS = Field.create("initial.sync.max.threads")
            .withDisplayName("Maximum number of threads for initial sync")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(1)
            .withValidation(Field::isPositiveInteger)
            .withDescription("Maximum number of threads used to perform an initial sync of the collections in a replica set. "
                    + "Defaults to 1.");

    public static final Field CONNECT_BACKOFF_INITIAL_DELAY_MS = Field.create("connect.backoff.initial.delay.ms")
            .withDisplayName("Initial delay before reconnection (ms)")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(TimeUnit.SECONDS.toMillis(1))
            .withValidation(Field::isPositiveInteger)
            .withDescription(
                    "The initial delay when trying to reconnect to a primary after a connection cannot be made or when no primary is available. Defaults to 1 second (1000 ms).");

    public static final Field CONNECT_BACKOFF_MAX_DELAY_MS = Field.create("connect.backoff.max.delay.ms")
            .withDisplayName("Maximum delay before reconnection (ms)")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(TimeUnit.SECONDS.toMillis(120))
            .withValidation(Field::isPositiveInteger)
            .withDescription(
                    "The maximum delay when trying to reconnect to a primary after a connection cannot be made or when no primary is available. Defaults to 120 second (120,000 ms).");

    public static final Field MAX_FAILED_CONNECTIONS = Field.create("connect.max.attempts")
            .withDisplayName("Connection attempt limit")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withImportance(Importance.HIGH)
            .withDefault(16)
            .withValidation(Field::isPositiveInteger)
            .withDescription("Maximum number of failed connection attempts to a replica set primary before an exception occurs and task is aborted. "
                    + "Defaults to 16, which with the defaults for '"
                    + CONNECT_BACKOFF_INITIAL_DELAY_MS + "' and '"
                    + CONNECT_BACKOFF_MAX_DELAY_MS + "' results in "
                    + "just over 20 minutes of attempts before failing.");

    public static final Field AUTO_DISCOVER_MEMBERS = Field.create("mongodb.members.auto.discover")
            .withDisplayName("Auto-discovery")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(true)
            .withValidation(Field::isBoolean)
            .withDescription("Specifies whether the addresses in 'hosts' are seeds that should be "
                    + "used to discover all members of the cluster or replica set ('true'), "
                    + "or whether the address(es) in 'hosts' should be used as is ('false'). "
                    + "The default is 'true'.");

    /**
     * A comma-separated list of regular expressions that match the databases to be monitored.
     * May not be used with {@link #DATABASE_BLACKLIST}.
     */
    public static final Field DATABASE_WHITELIST = Field.create("database.whitelist")
            .withDisplayName("DB Whitelist")
            .withType(Type.LIST)
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withValidation(Field::isListOfRegex,
                    MongoDbConnectorConfig::validateDatabaseBlacklist)
            .withDescription("The databases for which changes are to be captured");

    /**
     * A comma-separated list of regular expressions that match the databases to be excluded.
     * May not be used with {@link #DATABASE_WHITELIST}.
     */
    public static final Field DATABASE_BLACKLIST = Field.create("database.blacklist")
            .withDisplayName("DB Blacklist")
            .withType(Type.LIST)
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withValidation(Field::isListOfRegex)
            .withDescription("The databases for which changes are to be excluded");

    /**
     * A comma-separated list of regular expressions that match the fully-qualified namespaces of collections to be monitored.
     * Fully-qualified namespaces for collections are of the form {@code <databaseName>.<collectionName>}.
     * May not be used with {@link #COLLECTION_BLACKLIST}.
     */
    public static final Field COLLECTION_WHITELIST = Field.create("collection.whitelist")
            .withDisplayName("Collections")
            .withType(Type.LIST)
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withValidation(Field::isListOfRegex,
                    MongoDbConnectorConfig::validateCollectionBlacklist)
            .withDescription("The collections for which changes are to be captured");

    /**
     * A comma-separated list of regular expressions that match the fully-qualified namespaces of collections to be excluded from
     * monitoring. Fully-qualified namespaces for collections are of the form {@code <databaseName>.<collectionName>}.
     * May not be used with {@link #COLLECTION_WHITELIST}.
     */
    public static final Field COLLECTION_BLACKLIST = Field.create("collection.blacklist")
            .withValidation(Field::isListOfRegex)
            .withInvisibleRecommender();

    /**
     * A comma-separated list of the fully-qualified names of fields that should be excluded from change event message values.
     * Fully-qualified names for fields are of the form {@code
     * <databaseName>.<collectionName>.<fieldName>.<nestedFieldName>}, where {@code <databaseName>} and
     * {@code <collectionName>} may contain the wildcard ({@code *}) which matches any characters.
     */
    public static final Field FIELD_BLACKLIST = Field.create("field.blacklist")
            .withDisplayName("Exclude Fields")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDescription("");

    /**
     * A comma-separated list of the fully-qualified replacements of fields that should be used to rename fields in change
     * event message values. Fully-qualified replacements for fields are of the form {@code
     * <databaseName>.<collectionName>.<fieldName>.<nestedFieldName>:<newNestedFieldName>}, where
     * {@code <databaseName>} and {@code <collectionName>} may contain the wildcard ({@code *}) which matches
     * any characters, the colon character ({@code :}) is used to determine rename mapping of field.
     */
    public static final Field FIELD_RENAMES = Field.create("field.renames")
            .withDisplayName("Rename Fields")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDescription("");

    public static final Field SNAPSHOT_MODE = Field.create("snapshot.mode")
            .withDisplayName("Snapshot mode")
            .withEnum(SnapshotMode.class, SnapshotMode.INITIAL)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("The criteria for running a snapshot upon startup of the connector. "
                    + "Options include: "
                    + "'initial' (the default) to specify the connector should always perform an initial sync when required; "
                    + "'never' to specify the connector should never perform an initial sync ");

    public static final Field CONNECT_TIMEOUT_MS = Field.create("mongodb.connect.timeout.ms")
            .withDisplayName("Connect Timeout MS")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(10000)
            .withDescription("The connection timeout in milliseconds");

    public static final Field SERVER_SELECTION_TIMEOUT_MS = Field.create("mongodb.server.selection.timeout.ms")
            .withDisplayName("Server selection timeout MS")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(30000)
            .withDescription("The server selection timeout in milliseconds");

    public static final Field SOCKET_TIMEOUT_MS = Field.create("mongodb.socket.timeout.ms")
            .withDisplayName("Socket timeout MS")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(0)
            .withDescription("The socket timeout in milliseconds");

    protected static final Field TASK_ID = Field.create("mongodb.task.id")
            .withDescription("Internal use only")
            .withValidation(Field::isInteger)
            .withInvisibleRecommender();

    private static final ConfigDefinition CONFIG_DEFINITION = CommonConnectorConfig.CONFIG_DEFINITION.edit()
            .name("MongoDB")
            .type(
                    HOSTS,
                    USER,
                    PASSWORD,
                    AUTH_SOURCE,
                    LOGICAL_NAME,
                    CONNECT_BACKOFF_INITIAL_DELAY_MS,
                    CONNECT_BACKOFF_MAX_DELAY_MS,
                    CONNECT_TIMEOUT_MS,
                    SOCKET_TIMEOUT_MS,
                    SERVER_SELECTION_TIMEOUT_MS,
                    POLL_INTERVAL_SEC,
                    MONGODB_POLL_INTERVAL_MS,
                    MAX_FAILED_CONNECTIONS,
                    AUTO_DISCOVER_MEMBERS,
                    SSL_ENABLED,
                    SSL_ALLOW_INVALID_HOSTNAMES)
            .events(
                    DATABASE_WHITELIST,
                    DATABASE_BLACKLIST,
                    COLLECTION_WHITELIST,
                    COLLECTION_BLACKLIST,
                    FIELD_BLACKLIST,
                    FIELD_RENAMES)
            .connector(
                    MAX_COPY_THREADS,
                    SNAPSHOT_MODE)
            .create();

    /**
     * The set of {@link Field}s defined as part of this configuration.
     */
    public static Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    public static ConfigDef configDef() {
        return CONFIG_DEFINITION.configDef();
    }

    protected static Field.Set EXPOSED_FIELDS = ALL_FIELDS;

    private final SnapshotMode snapshotMode;

    public MongoDbConnectorConfig(Configuration config) {
        super(config, config.getString(LOGICAL_NAME), DEFAULT_SNAPSHOT_FETCH_SIZE);

        String snapshotModeValue = config.getString(MongoDbConnectorConfig.SNAPSHOT_MODE);
        this.snapshotMode = SnapshotMode.parse(snapshotModeValue, MongoDbConnectorConfig.SNAPSHOT_MODE.defaultValueAsString());
    }

    private static int validateHosts(Configuration config, Field field, ValidationOutput problems) {
        String hosts = config.getString(field);
        if (hosts == null) {
            problems.accept(field, hosts, "Host specification is required");
            return 1;
        }
        int count = 0;
        if (ReplicaSets.parse(hosts).all().isEmpty()) {
            problems.accept(field, hosts, "Invalid host specification");
            ++count;
        }
        return count;
    }

    private static int validateCollectionBlacklist(Configuration config, Field field, ValidationOutput problems) {
        return validateBlacklistField(config, problems, COLLECTION_WHITELIST, COLLECTION_BLACKLIST);
    }

    private static int validateDatabaseBlacklist(Configuration config, Field field, ValidationOutput problems) {
        return validateBlacklistField(config, problems, DATABASE_WHITELIST, DATABASE_BLACKLIST);
    }

    private static int validateBlacklistField(Configuration config, ValidationOutput problems, Field fieldWhitelist, Field fieldBlacklist) {
        String whitelist = config.getString(fieldWhitelist);
        String blacklist = config.getString(fieldBlacklist);
        if (whitelist != null && blacklist != null) {
            problems.accept(fieldBlacklist, blacklist, "Whitelist is already specified");
            return 1;
        }
        return 0;
    }

    public SnapshotMode getSnapshotMode() {
        return snapshotMode;
    }

    @Override
    protected SourceInfoStructMaker<? extends AbstractSourceInfo> getSourceInfoStructMaker(Version version) {
        switch (version) {
            case V1:
                return new LegacyV1MongoDbSourceInfoStructMaker(Module.name(), Module.version(), this);
            default:
                return new MongoDbSourceInfoStructMaker(Module.name(), Module.version(), this);
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
}
