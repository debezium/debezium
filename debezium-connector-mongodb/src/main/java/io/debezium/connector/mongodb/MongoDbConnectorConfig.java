/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.connect.data.Struct;
import org.bson.Document;
import org.bson.RawBsonDocument;
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
import io.debezium.data.Envelope;
import io.debezium.schema.DataCollectionId;

/**
 * The configuration properties.
 */
public class MongoDbConnectorConfig extends CommonConnectorConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbConnectorConfig.class);

    protected static final String COLLECTION_INCLUDE_LIST_ALREADY_SPECIFIED_ERROR_MSG = "\"collection.include.list\" is already specified";
    protected static final String COLLECTION_WHITELIST_ALREADY_SPECIFIED_ERROR_MSG = "\"collection.whitelist\" is already specified";
    protected static final String DATABASE_INCLUDE_LIST_ALREADY_SPECIFIED_ERROR_MSG = "\"database.include.list\" is already specified";
    protected static final String DATABASE_WHITELIST_ALREADY_SPECIFIED_ERROR_MSG = "\"database.whitelist\" is already specified";

    protected static final Pattern PATTERN_SPILT = Pattern.compile(",");

    protected static final Pattern FIELD_EXCLUDE_LIST_PATTERN = Pattern.compile("^[*|\\w|\\s*]+(?:\\.[\\w]+\\.[\\w]+)+(\\.[\\w]+)*\\s*$");
    protected static final String QUALIFIED_FIELD_EXCLUDE_LIST_PATTERN = "<databaseName>.<collectionName>.<fieldName>.<nestedFieldName>";
    protected static final Pattern FIELD_RENAMES_PATTERN = Pattern.compile("^[*|\\w|\\s*]+(?:\\.[\\w]+\\.[\\w]+)+(\\.[\\w]+)*:(?:[\\w]+)+\\s*$");
    protected static final String QUALIFIED_FIELD_RENAMES_PATTERN = "<databaseName>.<collectionName>.<fieldName>.<nestedFieldName>:<newNestedFieldName>";

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

    /**
     * The set off different ways how connector can capture changes.
     */
    public static enum CaptureMode implements EnumeratedValue {

        /**
         * The classic oplog based capturing.
         */
        OPLOG("oplog", false, false, false),

        /**
         * Change capture based on MongoDB Change Streams support.
         */
        CHANGE_STREAMS("change_streams", true, false, false),

        /**
         * Change capture based on MongoDB change Streams support.
         * The update message will contain the full document.
         */
        CHANGE_STREAMS_UPDATE_FULL("change_streams_update_full", true, true, false),

        /**
         * Change capture based on MongoDB Change Streams support with pre-image.
         * When applicable, the change event will include the full document before change.
         */
        CHANGE_STREAMS_WITH_PRE_IMAGE("change_streams_with_pre_image", true, false, true),

        /**
         * Change capture based on MongoDB change Streams support with pre-image.
         * When applicable, the change event will include the full document before change.
         * The update message will contain the full document.
         */
        CHANGE_STREAMS_UPDATE_FULL_WITH_PRE_IMAGE("change_streams_update_full_with_pre_image", true, true, true);

        private final String value;
        private final boolean changeStreams;
        private final boolean fullUpdate;
        private final boolean includePreImage;

        private CaptureMode(String value, boolean changeStreams, boolean fullUpdate, boolean includePreImage) {
            this.value = value;
            this.changeStreams = changeStreams;
            this.fullUpdate = fullUpdate;
            this.includePreImage = includePreImage;
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
        public static CaptureMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();

            for (CaptureMode option : CaptureMode.values()) {
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
        public static CaptureMode parse(String value, String defaultValue) {
            CaptureMode mode = parse(value);

            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }

            return mode;
        }

        public boolean isChangeStreams() {
            return changeStreams;
        }

        public boolean isFullUpdate() {
            return fullUpdate;
        }

        public boolean isIncludePreImage() {
            return includePreImage;
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
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 1))
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withValidation(MongoDbConnectorConfig::validateHosts)
            .withDescription("The hostname and port pairs (in the form 'host' or 'host:port') "
                    + "of the MongoDB server(s) in the replica set.");

    public static final Field LOGICAL_NAME = Field.create("mongodb.name")
            .withDisplayName("Namespace")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 0))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .required()
            .withDescription("Unique name that identifies the MongoDB replica set or cluster and all recorded offsets, and "
                    + "that is used as a prefix for all schemas and topics. "
                    + "Each distinct MongoDB installation should have a separate namespace and monitored by "
                    + "at most one Debezium connector.");

    public static final Field USER = Field.create("mongodb.user")
            .withDisplayName("User")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 3))
            .withWidth(Width.SHORT)
            .withImportance(Importance.HIGH)
            .withDescription("Database user for connecting to MongoDB, if necessary.");

    public static final Field PASSWORD = Field.create("mongodb.password")
            .withDisplayName("Password")
            .withType(Type.PASSWORD)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 4))
            .withWidth(Width.SHORT)
            .withImportance(Importance.HIGH)
            .withDescription("Password to be used when connecting to MongoDB, if necessary.");

    public static final Field AUTH_SOURCE = Field.create("mongodb.authsource")
            .withDisplayName("Credentials Database")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 3))
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
            .withDescription(
                    "(Deprecated, use mongodb.poll.interval.ms) Interval for looking for new, removed, or changed replica sets, given in seconds. Defaults to 30 seconds.");

    public static final Field MONGODB_POLL_INTERVAL_MS = Field.create("mongodb.poll.interval.ms")
            .withDisplayName("Replica membership poll interval (ms)")
            .withType(Type.LONG)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 5))
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(30_000L)
            .withValidation(Field::isPositiveInteger)
            .withDescription("Interval for looking for new, removed, or changed replica sets, given in milliseconds.  Defaults to 30 seconds (30,000 ms).");

    public static final Field SSL_ENABLED = Field.create("mongodb.ssl.enabled")
            .withDisplayName("Enable SSL connection to MongoDB")
            .withType(Type.BOOLEAN)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_SSL, 0))
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(false)
            .withValidation(Field::isBoolean)
            .withDescription("Should connector use SSL to connect to MongoDB instances");

    public static final Field SSL_ALLOW_INVALID_HOSTNAMES = Field.create("mongodb.ssl.invalid.hostname.allowed")
            .withDisplayName("Allow invalid hostnames for SSL connection")
            .withType(Type.BOOLEAN)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_SSL, 1))
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(false)
            .withValidation(Field::isBoolean)
            .withDescription("Whether invalid host names are allowed when using SSL. If true the connection will not prevent man-in-the-middle attacks");

    @Deprecated
    public static final Field MAX_COPY_THREADS = Field.create("initial.sync.max.threads")
            .withDisplayName("Maximum number of threads for initial sync")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(1)
            .withValidation(Field::isPositiveInteger)
            .withDescription("(Deprecated) Maximum number of threads used to perform an initial sync of the collections in a replica set. "
                    + "Defaults to 1.");

    public static final Field CONNECT_BACKOFF_INITIAL_DELAY_MS = Field.create("connect.backoff.initial.delay.ms")
            .withDisplayName("Initial delay before reconnection (ms)")
            .withType(Type.LONG)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 1))
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(TimeUnit.SECONDS.toMillis(1))
            .withValidation(Field::isPositiveInteger)
            .withDescription(
                    "The initial delay when trying to reconnect to a primary after a connection cannot be made or when no primary is available, given in milliseconds. Defaults to 1 second (1,000 ms).");

    public static final Field CONNECT_BACKOFF_MAX_DELAY_MS = Field.create("connect.backoff.max.delay.ms")
            .withDisplayName("Maximum delay before reconnection (ms)")
            .withType(Type.LONG)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 2))
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(TimeUnit.SECONDS.toMillis(120))
            .withValidation(Field::isPositiveInteger)
            .withDescription(
                    "The maximum delay when trying to reconnect to a primary after a connection cannot be made or when no primary is available, given in milliseconds. Defaults to 120 second (120,000 ms).");

    public static final Field MAX_FAILED_CONNECTIONS = Field.create("connect.max.attempts")
            .withDisplayName("Connection attempt limit")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 4))
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
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 2))
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
     * Must not be used with {@link #DATABASE_EXCLUDE_LIST}.
     */
    public static final Field DATABASE_INCLUDE_LIST = Field.create("database.include.list")
            .withDisplayName("Include Databases")
            .withType(Type.LIST)
            .withGroup(Field.createGroupEntry(Field.Group.FILTERS, 0))
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withValidation(Field::isListOfRegex)
            .withDescription("A comma-separated list of regular expressions that match the database names for which changes are to be captured");

    /**
     * Old, backwards-compatible "whitelist" property.
     */
    @Deprecated
    public static final Field DATABASE_WHITELIST = Field.create("database.whitelist")
            .withDisplayName("Deprecated: Include Databases")
            .withType(Type.LIST)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withValidation(Field::isListOfRegex)
            .withInvisibleRecommender()
            .withDescription("A comma-separated list of regular expressions that match the database names for which changes are to be captured (deprecated, use \""
                    + DATABASE_INCLUDE_LIST.name() + "\" instead)");

    /**
     * A comma-separated list of regular expressions that match the databases to be excluded.
     * Must not be used with {@link #DATABASE_INCLUDE_LIST}.
     */
    public static final Field DATABASE_EXCLUDE_LIST = Field.create("database.exclude.list")
            .withDisplayName("Exclude Databases")
            .withType(Type.LIST)
            .withGroup(Field.createGroupEntry(Field.Group.FILTERS, 1))
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withValidation(Field::isListOfRegex, MongoDbConnectorConfig::validateDatabaseExcludeList)
            .withDescription("A comma-separated list of regular expressions that match the database names for which changes are to be excluded");

    /**
     * Old, backwards-compatible "blacklist" property.
     */
    @Deprecated
    public static final Field DATABASE_BLACKLIST = Field.create("database.blacklist")
            .withDisplayName("Deprecated: Exclude Databases")
            .withType(Type.LIST)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withValidation(Field::isListOfRegex, MongoDbConnectorConfig::validateDatabaseBlacklist)
            .withInvisibleRecommender()
            .withDescription("A comma-separated list of regular expressions that match the database names for which changes are to be excluded (deprecated, use \""
                    + DATABASE_EXCLUDE_LIST.name() + "\" instead)");

    /**
     * A comma-separated list of regular expressions that match the fully-qualified namespaces of collections to be monitored.
     * Fully-qualified namespaces for collections are of the form {@code <databaseName>.<collectionName>}.
     * Must not be used with {@link #COLLECTION_EXCLUDE_LIST}.
     */
    public static final Field COLLECTION_INCLUDE_LIST = Field.create("collection.include.list")
            .withDisplayName("Include Collections")
            .withType(Type.LIST)
            .withGroup(Field.createGroupEntry(Field.Group.FILTERS, 2))
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withValidation(Field::isListOfRegex)
            .withDescription("A comma-separated list of regular expressions that match the collection names for which changes are to be captured");

    /**
     * Old, backwards-compatible "whitelist" property.
     */
    @Deprecated
    public static final Field COLLECTION_WHITELIST = Field.create("collection.whitelist")
            .withDisplayName("Deprecated: Include Collections")
            .withType(Type.LIST)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withValidation(Field::isListOfRegex)
            .withInvisibleRecommender()
            .withDescription("A comma-separated list of regular expressions that match the collection names for which changes are to be captured (deprecated, use \""
                    + COLLECTION_INCLUDE_LIST.name() + "\" instead)");

    /**
     * A comma-separated list of regular expressions that match the fully-qualified namespaces of collections to be excluded from
     * monitoring. Fully-qualified namespaces for collections are of the form {@code <databaseName>.<collectionName>}.
     * Must not be used with {@link #COLLECTION_INCLUDE_LIST}.
     */
    public static final Field COLLECTION_EXCLUDE_LIST = Field.create("collection.exclude.list")
            .withGroup(Field.createGroupEntry(Field.Group.FILTERS, 3))
            .withValidation(Field::isListOfRegex, MongoDbConnectorConfig::validateCollectionExcludeList)
            .withInvisibleRecommender()
            .withDescription("A comma-separated list of regular expressions that match the collection names for which changes are to be excluded");

    /**
     * Old, backwards-compatible "blacklist" property.
     */
    @Deprecated
    public static final Field COLLECTION_BLACKLIST = Field.create("collection.blacklist")
            .withValidation(Field::isListOfRegex, MongoDbConnectorConfig::validateCollectionBlacklist)
            .withInvisibleRecommender()
            .withDescription("A comma-separated list of regular expressions that match the collection names for which changes are to be excluded (deprecated, use \""
                    + COLLECTION_EXCLUDE_LIST.name() + "\" instead)");

    /**
     * A comma-separated list of the fully-qualified names of fields that should be excluded from change event message values.
     * Fully-qualified names for fields are of the form {@code
     * <databaseName>.<collectionName>.<fieldName>.<nestedFieldName>}, where {@code <databaseName>} and
     * {@code <collectionName>} may contain the wildcard ({@code *}) which matches any characters.
     */
    public static final Field FIELD_EXCLUDE_LIST = Field.create("field.exclude.list")
            .withDisplayName("Exclude Fields")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.FILTERS, 5))
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withValidation(MongoDbConnectorConfig::validateFieldExcludeList)
            .withDescription("A comma-separated list of the fully-qualified names of fields that should be excluded from change event message values");

    /**
     * Old, backwards-compatible "blacklist" property.
     */
    @Deprecated
    public static final Field FIELD_BLACKLIST = Field.create("field.blacklist")
            .withDisplayName("Deprecated: Exclude Fields")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withInvisibleRecommender()
            .withDescription("A comma-separated list of the fully-qualified names of fields that should be excluded from change event message values (deprecated, use \""
                    + FIELD_EXCLUDE_LIST.name() + "\" instead)");

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
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 0))
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withValidation(MongoDbConnectorConfig::validateFieldRenamesList)
            .withDescription("A comma-separated list of the fully-qualified replacements of fields that" +
                    " should be used to rename fields in change event message values. Fully-qualified replacements" +
                    " for fields are of the form databaseName.collectionName.fieldName.nestedFieldName:newNestedFieldName," +
                    " where databaseName and collectionName may contain the wildcard (*) which matches any characters," +
                    " the colon character (:) is used to determine rename mapping of field.");

    public static final Field SNAPSHOT_MODE = Field.create("snapshot.mode")
            .withDisplayName("Snapshot mode")
            .withEnum(SnapshotMode.class, SnapshotMode.INITIAL)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 0))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("The criteria for running a snapshot upon startup of the connector. "
                    + "Options include: "
                    + "'initial' (the default) to specify the connector should always perform an initial sync when required; "
                    + "'never' to specify the connector should never perform an initial sync ");

    public static final Field CAPTURE_MODE = Field.create("capture.mode")
            .withDisplayName("Capture mode")
            .withEnum(CaptureMode.class, CaptureMode.CHANGE_STREAMS_UPDATE_FULL)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 1))
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("The method used to capture changes from MongoDB server. "
                    + "Options include: "
                    + "'oplog' to capture changes from the oplog; "
                    + "'change_streams' to capture changes via MongoDB Change Streams, update events do not contain full documents; "
                    + "'change_streams_update_full' (the default) to capture changes via MongoDB Change Streams, update events contain full documents");

    public static final Field RAW_OPLOG_ENABLED = Field.create("mongodb.raw_oplog.enabled")
            .withDisplayName("Enable populate raw oplog for MongoDB")
            .withType(Type.BOOLEAN)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 1))
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(false)
            .withValidation(Field::isBoolean)
            .withDescription("If we populate raw_oplog field in the output. Should only use with capture.mode=oplog.");

    public static final Field STRIPE_AUDIT_FILTER_PATTERN = Field.create("mongodb.stripe_audit_filter_pattern")
            .withDisplayName("Regex pattern to do filtering on the stripeAudit field")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 1))
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDefault(false)
            .withDescription("Optional regex pattern to add filtering logic against the stripeAudit field");

    public static final Field ENABLE_BSON = Field.create("mongodb.enable_bson")
            .withDisplayName("Populate raw BSON for MongoDB")
            .withType(Type.BOOLEAN)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 1))
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(false)
            .withValidation(Field::isBoolean)
            .withDescription("Populate change events as raw BSON document instead of JSON.");

    public static final Field ALLOW_CMD_COLLECTION = Field.create("mongodb.allow_cmd_collection")
            .withDisplayName("Allow $cmd collection for MongoDB")
            .withType(Type.BOOLEAN)
            .withGroup(Field.createGroupEntry(Field.Group.FILTERS, 1))
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(false)
            .withValidation(Field::isBoolean)
            .withDescription("Allow $cmd collection when capture.mode=oplog.");

    public static final Field CONNECT_TIMEOUT_MS = Field.create("mongodb.connect.timeout.ms")
            .withDisplayName("Connect Timeout MS")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 0))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(10_000)
            .withDescription("The connection timeout, given in milliseconds. Defaults to 10 seconds (10,000 ms).");

    public static final Field SERVER_SELECTION_TIMEOUT_MS = Field.create("mongodb.server.selection.timeout.ms")
            .withDisplayName("Server selection timeout MS")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 5))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(30_000)
            .withDescription("The server selection timeout, given in milliseconds. Defaults to 10 seconds (10,000 ms).");

    public static final Field SOCKET_TIMEOUT_MS = Field.create("mongodb.socket.timeout.ms")
            .withDisplayName("Socket timeout MS")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 6))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(0)
            .withDescription("The socket timeout, given in milliseconds. Defaults to 0 ms.");

    public static final Field HEARTBEAT_FREQUENCY_MS = Field.create("mongodb.heartbeat.frequency.ms")
            .withDisplayName("Heartbeat frequency ms")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 7))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(10_000)
            .withDescription("The frequency that the cluster monitor attempts to reach each server. Defaults to 10 seconds (10,000 ms).");

    protected static final Field TASK_ID = Field.create("mongodb.task.id")
            .withDescription("Internal use only")
            .withValidation(Field::isInteger)
            .withInvisibleRecommender();

    public static final Field SNAPSHOT_FILTER_QUERY_BY_COLLECTION = Field.create("snapshot.collection.filter.overrides")
            .withDisplayName("Snapshot mode")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 1))
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDescription("This property contains a comma-separated list of <dbName>.<collectionName>, for which "
                    + " the initial snapshot may be a subset of data present in the data source. The subset would be defined"
                    + " by mongodb filter query specified as value for property snapshot.collection.filter.override.<dbname>.<collectionName>");

    public static final Field CURSOR_MAX_AWAIT_TIME_MS = Field.create("cursor.max.await.time.ms")
            .withDisplayName("Server's oplog streaming cursor max await time")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("The maximum processing time in milliseconds to wait for the oplog cursor to process a single poll request");

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
                    HEARTBEAT_FREQUENCY_MS,
                    SOCKET_TIMEOUT_MS,
                    SERVER_SELECTION_TIMEOUT_MS,
                    POLL_INTERVAL_SEC,
                    MONGODB_POLL_INTERVAL_MS,
                    MAX_FAILED_CONNECTIONS,
                    AUTO_DISCOVER_MEMBERS,
                    SSL_ENABLED,
                    SSL_ALLOW_INVALID_HOSTNAMES,
                    CURSOR_MAX_AWAIT_TIME_MS)
            .events(
                    DATABASE_WHITELIST,
                    DATABASE_INCLUDE_LIST,
                    DATABASE_BLACKLIST,
                    DATABASE_EXCLUDE_LIST,
                    COLLECTION_WHITELIST,
                    COLLECTION_INCLUDE_LIST,
                    COLLECTION_BLACKLIST,
                    COLLECTION_EXCLUDE_LIST,
                    FIELD_BLACKLIST,
                    FIELD_EXCLUDE_LIST,
                    FIELD_RENAMES,
                    SNAPSHOT_FILTER_QUERY_BY_COLLECTION)
            .connector(
                    MAX_COPY_THREADS,
                    SNAPSHOT_MODE,
                    CAPTURE_MODE,
                    SCHEMA_NAME_ADJUSTMENT_MODE)
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
    private CaptureMode captureMode;
    private final boolean enableRawOplog;
    private final boolean enableBson;
    private final boolean allowCmdCollection;
    private final int snapshotMaxThreads;
    private final int cursorMaxAwaitTimeMs;
    private final String stripeAuditFilterPattern;

    public MongoDbConnectorConfig(Configuration config) {
        super(config, config.getString(LOGICAL_NAME), DEFAULT_SNAPSHOT_FETCH_SIZE);

        String snapshotModeValue = config.getString(MongoDbConnectorConfig.SNAPSHOT_MODE);
        this.snapshotMode = SnapshotMode.parse(snapshotModeValue, MongoDbConnectorConfig.SNAPSHOT_MODE.defaultValueAsString());

        String captureModeValue = config.getString(MongoDbConnectorConfig.CAPTURE_MODE);
        this.captureMode = CaptureMode.parse(captureModeValue, MongoDbConnectorConfig.CAPTURE_MODE.defaultValueAsString());
        this.enableRawOplog = config.getBoolean(MongoDbConnectorConfig.RAW_OPLOG_ENABLED, false);
        this.enableBson = config.getBoolean(MongoDbConnectorConfig.ENABLE_BSON, false);
        this.allowCmdCollection = config.getBoolean(MongoDbConnectorConfig.ALLOW_CMD_COLLECTION, false);
        this.stripeAuditFilterPattern = config.getString(MongoDbConnectorConfig.STRIPE_AUDIT_FILTER_PATTERN, "");

        this.snapshotMaxThreads = resolveSnapshotMaxThreads(config);
        this.cursorMaxAwaitTimeMs = config.getInteger(MongoDbConnectorConfig.CURSOR_MAX_AWAIT_TIME_MS, 0);
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

    private static int validateFieldExcludeList(Configuration config, Field field, ValidationOutput problems) {
        int problemCount = 0;
        String fieldExcludeList = config.getString(FIELD_EXCLUDE_LIST);

        if (fieldExcludeList != null) {
            for (String excludeField : PATTERN_SPILT.split(fieldExcludeList)) {
                if (!FIELD_EXCLUDE_LIST_PATTERN.asPredicate().test(excludeField)) {
                    problems.accept(FIELD_EXCLUDE_LIST, excludeField, excludeField + " has invalid format (expecting " + QUALIFIED_FIELD_EXCLUDE_LIST_PATTERN + ")");
                    problemCount++;
                }
            }
        }
        return problemCount;
    }

    private static int validateFieldRenamesList(Configuration config, Field field, ValidationOutput problems) {
        int problemCount = 0;
        String fieldRenamesList = config.getString(FIELD_RENAMES);

        if (fieldRenamesList != null) {
            for (String renameField : PATTERN_SPILT.split(fieldRenamesList)) {
                if (!FIELD_RENAMES_PATTERN.asPredicate().test(renameField)) {
                    problems.accept(FIELD_EXCLUDE_LIST, renameField, renameField + " has invalid format (expecting " + QUALIFIED_FIELD_RENAMES_PATTERN + ")");
                    problemCount++;
                }
            }
        }
        return problemCount;
    }

    private static int validateCollectionExcludeList(Configuration config, Field field, ValidationOutput problems) {
        String includeList = config.getString(COLLECTION_INCLUDE_LIST);
        String excludeList = config.getString(COLLECTION_EXCLUDE_LIST);
        if (includeList != null && excludeList != null) {
            problems.accept(COLLECTION_EXCLUDE_LIST, excludeList, COLLECTION_INCLUDE_LIST_ALREADY_SPECIFIED_ERROR_MSG);
            return 1;
        }
        return 0;
    }

    private static int validateCollectionBlacklist(Configuration config, Field field, ValidationOutput problems) {
        String whitelist = config.getFallbackStringPropertyWithWarning(COLLECTION_INCLUDE_LIST, COLLECTION_WHITELIST);
        String blacklist = config.getFallbackStringPropertyWithWarning(COLLECTION_EXCLUDE_LIST, COLLECTION_BLACKLIST);
        if (whitelist != null && blacklist != null) {
            problems.accept(COLLECTION_EXCLUDE_LIST, blacklist, COLLECTION_INCLUDE_LIST_ALREADY_SPECIFIED_ERROR_MSG);
            return 1;
        }
        return 0;
    }

    private static int validateDatabaseExcludeList(Configuration config, Field field, ValidationOutput problems) {
        String includeList = config.getString(DATABASE_INCLUDE_LIST);
        String excludeList = config.getString(DATABASE_EXCLUDE_LIST);
        if (includeList != null && excludeList != null) {
            problems.accept(DATABASE_EXCLUDE_LIST, excludeList, DATABASE_INCLUDE_LIST_ALREADY_SPECIFIED_ERROR_MSG);
            return 1;
        }
        return 0;
    }

    private static int validateDatabaseBlacklist(Configuration config, Field field, ValidationOutput problems) {
        String whitelist = config.getFallbackStringPropertyWithWarning(DATABASE_INCLUDE_LIST, DATABASE_WHITELIST);
        String blacklist = config.getFallbackStringPropertyWithWarning(DATABASE_EXCLUDE_LIST, DATABASE_BLACKLIST);
        if (whitelist != null && blacklist != null) {
            problems.accept(DATABASE_BLACKLIST, blacklist, DATABASE_WHITELIST_ALREADY_SPECIFIED_ERROR_MSG);
            return 1;
        }
        return 0;
    }

    public SnapshotMode getSnapshotMode() {
        return snapshotMode;
    }

    /**
     * Provides statically configured capture mode. The configured value can be overrided upon
     * connector start if offsets stored were created by a different capture mode.
     *
     * See {@link MongoDbTaskContext#getCaptureMode()}
     *
     * @return capture mode requested by configuration
     */
    public CaptureMode getCaptureMode() {
        return captureMode;
    }

    public boolean getEnableRawOplog() {
        return enableRawOplog;
    }

    public String getStripeAuditFilterPattern() {
        return stripeAuditFilterPattern;
    }

    public boolean getEnableBson() {
        return enableBson;
    }

    public boolean getAllowCmdCollection() {
        LOGGER.debug("isChangeStreams: {} disableOperationFilter {}", captureMode.isChangeStreams(), allowCmdCollection);
        return !captureMode.isChangeStreams() && allowCmdCollection;
    }

    public int getCursorMaxAwaitTime() {
        return cursorMaxAwaitTimeMs;
    }

    @Override
    public int getSnapshotMaxThreads() {
        return snapshotMaxThreads;
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

    public Optional<String> getSnapshotFilterQueryForCollection(CollectionId collectionId) {
        return Optional.ofNullable(getSnapshotFilterQueryByCollection().get(collectionId.dbName() + "." + collectionId.name()));
    }

    public Map<String, String> getSnapshotFilterQueryByCollection() {
        String collectionList = getConfig().getString(SNAPSHOT_FILTER_QUERY_BY_COLLECTION);

        if (collectionList == null) {
            return Collections.emptyMap();
        }

        Map<String, String> snapshotFilterQueryByCollection = new HashMap<>();

        for (String collection : collectionList.split(",")) {
            snapshotFilterQueryByCollection.put(
                    collection,
                    getConfig().getString(
                            new StringBuilder().append(SNAPSHOT_FILTER_QUERY_BY_COLLECTION).append(".")
                                    .append(collection).toString()));
        }

        return Collections.unmodifiableMap(snapshotFilterQueryByCollection);

    }

    @Override
    public boolean supportsOperationFiltering() {
        return true;
    }

    @Override
    public String getContextName() {
        return Module.contextName();
    }

    @Override
    public String getConnectorName() {
        return Module.name();
    }

    private static int resolveSnapshotMaxThreads(Configuration config) {
        if (config.hasKey(SNAPSHOT_MAX_THREADS.name())) {
            return config.getInteger(SNAPSHOT_MAX_THREADS);
        }
        else {
            if (config.hasKey(MAX_COPY_THREADS.name())) {
                LOGGER.warn("The option '{}' is deprecated.  Use '{}' instead.", MAX_FAILED_CONNECTIONS.name(), SNAPSHOT_MAX_THREADS.name());
            }
            return config.getInteger(MAX_COPY_THREADS);
        }
    }

    @Override
    public Optional<String[]> parseSignallingMessage(Struct value) {
        final String after;
        if (getEnableBson()) {
            RawBsonDocument data = new RawBsonDocument(value.getBytes(Envelope.FieldName.AFTER));
            after = data.toJson();
        }
        else {
            after = value.getString(Envelope.FieldName.AFTER);
        }

        if (after == null) {
            LOGGER.warn("After part of signal '{}' is missing", value);
            return Optional.empty();
        }
        final Document fields = Document.parse(after);
        if (fields.size() != 3) {
            LOGGER.warn("The signal event '{}' should have 3 fields but has {}", after, fields.size());
            return Optional.empty();
        }
        final String[] result = new String[3];
        int idx = 0;
        for (Object fieldValue : fields.values()) {
            if (fieldValue instanceof Document) {
                result[idx++] = ((Document) fieldValue).toJson();
            }
            else {
                result[idx++] = fieldValue.toString();
            }
        }
        return Optional.of(result);
    }

    @Override
    public boolean isSignalDataCollection(DataCollectionId dataCollectionId) {
        final CollectionId id = (CollectionId) dataCollectionId;
        return getSignalingDataCollectionId() != null
                && getSignalingDataCollectionId().equals(id.dbName() + "." + id.name());
    }
}
