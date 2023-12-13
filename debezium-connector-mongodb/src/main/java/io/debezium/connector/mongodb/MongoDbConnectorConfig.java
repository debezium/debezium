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
import java.util.regex.Pattern;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.connect.data.Struct;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ConnectionString;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.config.Field.ValidationOutput;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.connector.mongodb.connection.ConnectionStrings;
import io.debezium.connector.mongodb.connection.DefaultMongoDbAuthProvider;
import io.debezium.connector.mongodb.connection.ReplicaSet;
import io.debezium.data.Envelope;
import io.debezium.schema.DefaultTopicNamingStrategy;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Strings;

/**
 * The configuration properties.
 */
public class MongoDbConnectorConfig extends CommonConnectorConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbConnectorConfig.class);

    protected static final String COLLECTION_INCLUDE_LIST_ALREADY_SPECIFIED_ERROR_MSG = "\"collection.include.list\" is already specified";
    protected static final String DATABASE_INCLUDE_LIST_ALREADY_SPECIFIED_ERROR_MSG = "\"database.include.list\" is already specified";

    protected static final Pattern PATTERN_SPILT = Pattern.compile(",");

    protected static final Pattern FIELD_EXCLUDE_LIST_PATTERN = Pattern
            .compile("^[*|\\w|\\-|\\s*]+(?:\\.[*|\\w|\\-]+\\.[*|\\w|\\-]+)+(\\.[*|\\w|\\-]+)*\\s*$");
    protected static final String QUALIFIED_FIELD_EXCLUDE_LIST_PATTERN = "<databaseName>.<collectionName>.<fieldName>.<nestedFieldName>";
    protected static final Pattern FIELD_RENAMES_PATTERN = Pattern
            .compile("^[*|\\w|\\-|\\s*]+(?:\\.[*|\\w|\\-]+\\.[*|\\w|\\-]+)+(\\.[*|\\w|\\-]+)*:(?:[*|\\w|\\-]+)+\\s*$");
    protected static final String QUALIFIED_FIELD_RENAMES_PATTERN = "<databaseName>.<collectionName>.<fieldName>.<nestedFieldName>:<newNestedFieldName>";

    /**
     * The set of predefined SnapshotMode options or aliases.
     */
    public enum SnapshotMode implements EnumeratedValue {

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

        SnapshotMode(String value, boolean includeData) {
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
    public enum CaptureMode implements EnumeratedValue {

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

        CaptureMode(String value, boolean changeStreams, boolean fullUpdate, boolean includePreImage) {
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

        public boolean isFullUpdate() {
            return fullUpdate;
        }

        public boolean isIncludePreImage() {
            return includePreImage;
        }
    }

    /**
     * The set of predefined CaptureScope options or aliases.
     */
    public enum CaptureScope implements EnumeratedValue {
        /**
         * Capture changes from entire MongoDB deployment
         * <p>
         * The MongoDB user used by debezium needs the following permissions/roles
         * <ul>
         *     <li>read role for any database
         *     <li>read permissions to the config.shards collection (for sharded clusters with connection.mode=replica_set)</li>
         * </ul>
         */
        DEPLOYMENT("deployment"),

        /**
         * Capture changes from database.
         * <p>
         * The MongoDB user used by debezium needs the following permissions/roles
         * <ul>
         *     <li>read role for database specified by {@link MongoDbConnectorConfig#CAPTURE_TARGET}</li>
         *     <li>write permissions to the signalling collection</li>
         *     <li>read permissions to the config.shards collection (for sharded clusters with connection.mode=replica_set)</li>
         * </ul>
         *
         * Additionally, the signaling collection has to reside under {@link MongoDbConnectorConfig#CAPTURE_TARGET}
         */
        DATABASE("database");

        private final String value;

        CaptureScope(String value) {
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
        public static CaptureScope parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();

            for (CaptureScope option : CaptureScope.values()) {
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
        public static CaptureScope parse(String value, String defaultValue) {
            CaptureScope mode = parse(value);

            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }

            return mode;
        }
    }

    /**
     * The set of predefined CursorPipelineOrder options or aliases.
     */
    public enum CursorPipelineOrder implements EnumeratedValue {
        /**
         * Internal stages first, then user stages
         */
        INTERNAL_FIRST("internal_first"),

        /**
         * User stages first, then internal stages
         */
        USER_FIRST("user_first"),

        /**
         * Only user stages (replacing internal stages)
         */
        USER_ONLY("user_only");

        private String value;

        CursorPipelineOrder(String value) {
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
        public static CursorPipelineOrder parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();

            for (CursorPipelineOrder option : CursorPipelineOrder.values()) {
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
        public static CursorPipelineOrder parse(String value, String defaultValue) {
            CursorPipelineOrder mode = parse(value);

            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }

            return mode;
        }
    }

    /**
     * The set of predefined CursorPipelineOrder options or aliases.
     */
    public enum FiltersMatchMode implements EnumeratedValue {
        /**
         * Match by regex (use fully qualified name for collections)
         */
        REGEX("regex"),

        /**
         * Match by simple comparison (use simple name for collections)
         */
        LITERAL("literal");

        private String value;

        FiltersMatchMode(String value) {
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
        public static FiltersMatchMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();

            for (FiltersMatchMode option : FiltersMatchMode.values()) {
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
        public static FiltersMatchMode parse(String value, String defaultValue) {
            FiltersMatchMode mode = parse(value);

            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }

            return mode;
        }
    }

    /**
     * The set of predefined OversizeHandlingMode options or aliases.
     */
    public enum OversizeHandlingMode implements EnumeratedValue {
        /**
         * Fail if oversized event is encoutered
         */
        FAIL("fail"),

        /**
         * Skip oversized events
         */
        SKIP("skip"),

        /**
         * Split oversized events (only supported for MongoDB 6.0.9 and later
         */
        SPLIT("split");

        private String value;

        OversizeHandlingMode(String value) {
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
        public static OversizeHandlingMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();

            for (OversizeHandlingMode option : OversizeHandlingMode.values()) {
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
        public static OversizeHandlingMode parse(String value, String defaultValue) {
            OversizeHandlingMode mode = parse(value);

            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }

            return mode;
        }
    }

    protected static final int DEFAULT_SNAPSHOT_FETCH_SIZE = 0;

    /**
     * The task connection string
     */
    public static final Field TASK_CONNECTION_STRING = Field.createInternal("mongodb.internal.task.connection.string")
            .withDescription("Internal use only")
            .withType(Type.LIST);


    public static final Field ALLOW_OFFSET_INVALIDATION = Field.createInternal("mongodb.allow.offset.invalidation")
            .withDescription("Allows offset invalidation when required by change of connection mode")
            .withDefault(false)
            .withType(Type.BOOLEAN);

    // MongoDb fields in Connection Group start from 1 (topic.prefix is 0)
    public static final Field CONNECTION_STRING = Field.create("mongodb.connection.string")
            .withDisplayName("Connection String")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 1))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withValidation(MongoDbConnectorConfig::validateConnectionString)
            .withDescription("Database connection string.");

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

    public static final Field MONGODB_POLL_INTERVAL_MS = Field.create("mongodb.poll.interval.ms")
            .withDisplayName("Replica membership poll interval (ms)")
            .withType(Type.LONG)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 5))
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(30_000L)
            .withValidation(Field::isPositiveInteger)
            .withDescription("Interval for looking for new, removed, or changed replica sets, given in milliseconds. Defaults to 30 seconds (30,000 ms).");

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

    public static final Field CONNECT_TIMEOUT_MS = Field.create("mongodb.connect.timeout.ms")
            .withDisplayName("Connect Timeout MS")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 0))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(10_000)
            .withDescription("The connection timeout, given in milliseconds. Defaults to 10 seconds (10,000 ms).");

    public static final Field AUTH_SOURCE = Field.create("mongodb.authsource")
            .withDisplayName("Credentials Database")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 1))
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(ReplicaSetDiscovery.ADMIN_DATABASE_NAME)
            .withDescription("Database containing user credentials.");

    public static final Field SERVER_SELECTION_TIMEOUT_MS = Field.create("mongodb.server.selection.timeout.ms")
            .withDisplayName("Server selection timeout MS")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 2))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(30_000)
            .withDescription("The server selection timeout, given in milliseconds. Defaults to 10 seconds (10,000 ms).");

    public static final Field SOCKET_TIMEOUT_MS = Field.create("mongodb.socket.timeout.ms")
            .withDisplayName("Socket timeout MS")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 3))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(0)
            .withDescription("The socket timeout, given in milliseconds. Defaults to 0 ms.");

    public static final Field HEARTBEAT_FREQUENCY_MS = Field.create("mongodb.heartbeat.frequency.ms")
            .withDisplayName("Heartbeat frequency ms")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 4))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(10_000)
            .withDescription("The frequency that the cluster monitor attempts to reach each server. Defaults to 10 seconds (10,000 ms).");

    public static final Field AUTH_PROVIDER_CLASS = Field.create("mongodb.authentication.class")
            .withDisplayName("Authentication Provider Custom Class")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 5))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDefault(DefaultMongoDbAuthProvider.class.getName())
            .withDescription(
                    "This class must implement the 'MongoDbAuthProvider' interface and is called on each app boot to provide the MongoDB credentials from the provided config.");

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
            .withValidation(MongoDbConnectorConfig::validateListOfRegexesOrLiterals)
            .withDescription("A comma-separated list of regular expressions or literals that match the database names for which changes are to be captured");

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
            .withValidation(MongoDbConnectorConfig::validateListOfRegexesOrLiterals, MongoDbConnectorConfig::validateDatabaseExcludeList)
            .withDescription("A comma-separated list of regular expressions or literals that match the database names for which changes are to be excluded");

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
            .withValidation(MongoDbConnectorConfig::validateListOfRegexesOrLiterals)
            .withDescription("A comma-separated list of regular expressions or literals that match the collection names for which changes are to be captured");

    /**
     * A comma-separated list of regular expressions that match the fully-qualified namespaces of collections to be excluded from
     * monitoring. Fully-qualified namespaces for collections are of the form {@code <databaseName>.<collectionName>}.
     * Must not be used with {@link #COLLECTION_INCLUDE_LIST}.
     */
    public static final Field COLLECTION_EXCLUDE_LIST = Field.create("collection.exclude.list")
            .withGroup(Field.createGroupEntry(Field.Group.FILTERS, 3))
            .withValidation(MongoDbConnectorConfig::validateListOfRegexesOrLiterals, MongoDbConnectorConfig::validateCollectionExcludeList)
            .withInvisibleRecommender()
            .withDescription("A comma-separated list of regular expressions or literals that match the collection names for which changes are to be excluded");

    public static final Field FILTERS_MATCH_MODE = Field.create("filters.match.mode")
            .withDisplayName("Database and collection include/exclude match mode")
            .withEnum(FiltersMatchMode.class, FiltersMatchMode.REGEX)
            .withGroup(Field.createGroupEntry(Field.Group.FILTERS, 6))
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("The mode used by the aggregation pipeline to match events based on included/excluded database and collection names"
                    + "Options include: "
                    + "'regex' (the default) Database and collection includes/excludes are evaluated as regular expressions; "
                    + "'literal' Database and collection includes/excludes are evaluated as comma-separated list of string literals; ");

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

    public static final Field CAPTURE_MODE = Field.create("capture.mode")
            .withDisplayName("Capture mode")
            .withEnum(CaptureMode.class, CaptureMode.CHANGE_STREAMS_UPDATE_FULL)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 1))
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("The method used to capture changes from MongoDB server. "
                    + "Options include: "
                    + "'change_streams' to capture changes via MongoDB Change Streams, update events do not contain full documents; "
                    + "'change_streams_update_full' (the default) to capture changes via MongoDB Change Streams, update events contain full documents");

    public static final Field CAPTURE_SCOPE = Field.create("capture.scope")
            .withDisplayName("Capture scope")
            .withEnum(CaptureScope.class, CaptureScope.DEPLOYMENT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 2))
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("The scope of captured changes. "
                    + "Options include: "
                    + "'deployment' (the default) to capture changes from the entire MongoDB deployment; "
                    + "'database' to capture changes from a specific MongoDB database");

    public static final Field CAPTURE_TARGET = Field.create("capture.target")
            .withDisplayName("Capture target")
            .withType(Type.STRING)
            .withValidation(MongoDbConnectorConfig::validateCaptureTarget)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 3))
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Name of captured database for " + CAPTURE_SCOPE.name() + "=" + CaptureScope.DATABASE.value);

    protected static final Field TASK_ID = Field.create("mongodb.task.id")
            .withDescription("Internal use only")
            .withValidation(Field::isInteger)
            .withInvisibleRecommender();

    public static final Field SNAPSHOT_MODE = Field.create("snapshot.mode")
            .withDisplayName("Snapshot mode")
            .withEnum(SnapshotMode.class, SnapshotMode.INITIAL)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 0))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("The criteria for running a snapshot upon startup of the connector. "
                    + "Select one of the following snapshot options: "
                    + "'initial' (default):  If the connector does not detect any offsets for the logical server name, it runs a snapshot that captures the current full state of the configured tables. After the snapshot completes, the connector begins to stream changes from the oplog. "
                    + "'never': The connector does not run a snapshot. Upon first startup, the connector immediately begins reading from the beginning of the oplog.");

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

    public static final Field CURSOR_PIPELINE = Field.create("cursor.pipeline")
            .withDisplayName("Pipeline stages applied to the change stream cursor")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 4))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withValidation(MongoDbConnectorConfig::validateChangeStreamPipeline)
            .withDescription("Applies processing to change events as part of the the standard MongoDB aggregation stream pipeline. " +
                    "A pipeline is a MongoDB aggregation pipeline composed of instructions to the database to filter or transform data. " +
                    "This can be used customize the data that the connector consumes. " +
                    "Note that this comes after the internal pipelines used to support the connector (e.g. filtering database and collection names).");

    public static final Field CURSOR_PIPELINE_ORDER = Field.create("cursor.pipeline.order")
            .withDisplayName("Change stream cursor pipeline order")
            .withEnum(CursorPipelineOrder.class, CursorPipelineOrder.INTERNAL_FIRST)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 5))
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("The order used to construct the effective MongoDB aggregation stream pipeline "
                    + "Options include: "
                    + "'internal_first' (the default) Internal stages defined by the connector are applied first; "
                    + "'user_first' Stages defined by the 'cursor.pipeline' property are applied first; "
                    + "'user_only' Stages defined by the 'cursor.pipeline' property will replace internal stages defined by the connector; ");

    public static final Field CURSOR_OVERSIZE_HANDLING_MODE = Field.create("cursor.oversize.handling.mode")
            .withDisplayName("Oversize document handling mode")
            .withEnum(OversizeHandlingMode.class, OversizeHandlingMode.FAIL)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 6))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("The strategy used to handle change events for documents exceeding specified BSON size. "
                    + "Options include: "
                    + "'fail' (the default) the connector fails if the total size of change event exceed the maximum BSON size"
                    + "'skip' any change events for documents exceeding the maximum size will be ignored"
                    + "'split' change events exceeding the maximum BSON size will be split using the $changeStreamSplitLargeEvent aggregation");

    public static final Field CURSOR_OVERSIZE_SKIP_THRESHOLD = Field.create("cursor.oversize.skip.threshold")
            .withDisplayName("Oversize document skip threshold")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 7))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(0)
            .withValidation(MongoDbConnectorConfig::validateOversizeSkipThreshold)
            .withDescription("The maximum allowed size of the stored document for which change events are processed. "
                    + "This includes both, the size before and after database operation, "
                    + "more specifically this limits the size of fullDocument and fullDocumentBeforeChange filed of MongoDB change events.");

    public static final Field TOPIC_NAMING_STRATEGY = Field.create("topic.naming.strategy")
            .withDisplayName("Topic naming strategy class")
            .withType(Type.CLASS)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDescription("The name of the TopicNamingStrategy class that should be used to determine the topic name " +
                    "for data change, schema change, transaction, heartbeat event etc.")
            .withDefault(DefaultTopicNamingStrategy.class.getName());

    public static final Field SOURCE_INFO_STRUCT_MAKER = CommonConnectorConfig.SOURCE_INFO_STRUCT_MAKER
            .withDefault(MongoDbSourceInfoStructMaker.class.getName());

    private static final ConfigDefinition CONFIG_DEFINITION = CommonConnectorConfig.CONFIG_DEFINITION.edit()
            .name("MongoDB")
            .type(
                    TOPIC_PREFIX,
                    CONNECTION_STRING,
                    ALLOW_OFFSET_INVALIDATION,
                    USER,
                    PASSWORD,
                    AUTH_SOURCE,
                    CONNECT_TIMEOUT_MS,
                    HEARTBEAT_FREQUENCY_MS,
                    SOCKET_TIMEOUT_MS,
                    SERVER_SELECTION_TIMEOUT_MS,
                    MONGODB_POLL_INTERVAL_MS,
                    SSL_ENABLED,
                    SSL_ALLOW_INVALID_HOSTNAMES,
                    CURSOR_MAX_AWAIT_TIME_MS)
            .events(
                    DATABASE_INCLUDE_LIST,
                    DATABASE_EXCLUDE_LIST,
                    COLLECTION_INCLUDE_LIST,
                    COLLECTION_EXCLUDE_LIST,
                    FIELD_EXCLUDE_LIST,
                    FIELD_RENAMES,
                    SNAPSHOT_FILTER_QUERY_BY_COLLECTION,
                    SOURCE_INFO_STRUCT_MAKER)
            .connector(
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
    private final CaptureScope captureScope;
    private final String captureTarget;
    private final boolean offsetInvalidationAllowed;
    private final int snapshotMaxThreads;
    private final int cursorMaxAwaitTimeMs;
    private final ReplicaSet replicaSet;
    private final CursorPipelineOrder cursorPipelineOrder;
    private final OversizeHandlingMode oversizeHandlingMode;
    private final FiltersMatchMode filtersMatchMode;
    private final int oversizeSkipThreshold;

    public MongoDbConnectorConfig(Configuration config) {
        super(config, DEFAULT_SNAPSHOT_FETCH_SIZE);

        String snapshotModeValue = config.getString(MongoDbConnectorConfig.SNAPSHOT_MODE);
        this.snapshotMode = SnapshotMode.parse(snapshotModeValue, MongoDbConnectorConfig.SNAPSHOT_MODE.defaultValueAsString());

        String captureModeValue = config.getString(MongoDbConnectorConfig.CAPTURE_MODE);
        this.captureMode = CaptureMode.parse(captureModeValue, MongoDbConnectorConfig.CAPTURE_MODE.defaultValueAsString());

        this.offsetInvalidationAllowed = config.getBoolean(ALLOW_OFFSET_INVALIDATION);

        String captureScopeValue = config.getString(MongoDbConnectorConfig.CAPTURE_SCOPE);
        this.captureScope = CaptureScope.parse(captureScopeValue, MongoDbConnectorConfig.CAPTURE_SCOPE.defaultValueAsString());
        this.captureTarget = config.getString(MongoDbConnectorConfig.CAPTURE_TARGET);

        String cursorPipelineOrderValue = config.getString(MongoDbConnectorConfig.CURSOR_PIPELINE_ORDER);
        this.cursorPipelineOrder = CursorPipelineOrder.parse(cursorPipelineOrderValue, MongoDbConnectorConfig.CURSOR_PIPELINE_ORDER.defaultValueAsString());

        String oversizeHandlingModeValue = config.getString(MongoDbConnectorConfig.CURSOR_OVERSIZE_HANDLING_MODE);
        this.oversizeHandlingMode = OversizeHandlingMode.parse(oversizeHandlingModeValue, MongoDbConnectorConfig.CURSOR_OVERSIZE_HANDLING_MODE.defaultValueAsString());
        this.oversizeSkipThreshold = config.getInteger(CURSOR_OVERSIZE_SKIP_THRESHOLD);

        String filterMatchModeValue = config.getString(MongoDbConnectorConfig.FILTERS_MATCH_MODE);
        this.filtersMatchMode = FiltersMatchMode.parse(filterMatchModeValue, MongoDbConnectorConfig.FILTERS_MATCH_MODE.defaultValueAsString());

        this.snapshotMaxThreads = resolveSnapshotMaxThreads(config);
        this.cursorMaxAwaitTimeMs = config.getInteger(MongoDbConnectorConfig.CURSOR_MAX_AWAIT_TIME_MS, 0);

        this.replicaSet = resolveReplicaSet(config);
    }

    private static int validateHosts(Configuration config, Field field, ValidationOutput problems) {
        String hosts = config.getString(field);
        String connectionString = config.getString(CONNECTION_STRING);

        if (hosts == null) {
            return 0;
        }

        LOGGER.warn("Config property '{}' will be removed in the future, use '{}' instead", field.name(), CONNECTION_STRING.name());

        if (connectionString != null) {
            LOGGER.warn("Config property '{}' is ignored, property '{}' takes precedence", field.name(), CONNECTION_STRING.name());
            return 0;
        }

        if (ConnectionStrings.parseFromHosts(hosts).isEmpty()) {
            problems.accept(field, null, "Invalid host specification");
            return 1;
        }

        return 0;
    }

    private static int validateChangeStreamPipeline(Configuration config, Field field, ValidationOutput problems) {
        String value = config.getString(field);

        try {
            new ChangeStreamPipeline(value);
        }
        catch (Exception e) {
            problems.accept(field, value, "Change stream pipeline JSON is invalid: " + e.getMessage());
            return 1;
        }
        return 0;
    }

    private static int validateOversizeSkipThreshold(Configuration config, Field field, ValidationOutput problems) {
        String mode = config.getString(CURSOR_OVERSIZE_HANDLING_MODE);
        int value = config.getInteger(CURSOR_OVERSIZE_SKIP_THRESHOLD);

        if (OversizeHandlingMode.SKIP.getValue().equals(mode) && value <= 0) {
            problems.accept(field, value, "Invalid threshold value for skipped document size");
            return 1;
        }

        return 0;
    }

    private static int validateConnectionString(Configuration config, Field field, ValidationOutput problems) {
        String connectionStringValue = config.getString(field);

        if (connectionStringValue == null) {
            problems.accept(field, null, "Missing connection string");
            return 1;
        }

        try {
            ConnectionString cs = new ConnectionString(connectionStringValue);
        }
        catch (Exception e) {
            problems.accept(field, connectionStringValue, "Invalid connection string");
            return 1;
        }
        return 0;
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

    private static int validateListOfRegexesOrLiterals(Configuration configuration, Field field, ValidationOutput problems) {
        var matchMode = configuration.getString(FILTERS_MATCH_MODE);

        if (matchMode != null && matchMode.equals(FiltersMatchMode.REGEX.getValue())) {
            return Field.isListOfRegex(configuration, field, problems);
        }

        var value = configuration.getString(field);
        var list = Strings.listOf(value, v -> v.split(","), String::trim);

        if (list.stream().anyMatch(String::isEmpty)) {
            problems.accept(field, value, field.name() + " contains empty values");
            return 1;
        }

        return 0;
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

    private static int validateDatabaseExcludeList(Configuration config, Field field, ValidationOutput problems) {
        String includeList = config.getString(DATABASE_INCLUDE_LIST);
        String excludeList = config.getString(DATABASE_EXCLUDE_LIST);
        if (includeList != null && excludeList != null) {
            problems.accept(DATABASE_EXCLUDE_LIST, excludeList, DATABASE_INCLUDE_LIST_ALREADY_SPECIFIED_ERROR_MSG);
            return 1;
        }
        return 0;
    }

    private static int validateCaptureTarget(Configuration config, Field field, ValidationOutput problems) {
        var value = config.getString(field);
        var scope = config.getString(MongoDbConnectorConfig.CAPTURE_SCOPE);

        if (value != null && CaptureScope.DEPLOYMENT.value.equals(scope)) {
            LOGGER.warn("Config property '{}' will be ignored due to {}={}", field.name(), CAPTURE_SCOPE.name(), scope);
        }

        if (value == null) {
            problems.accept(field, null, field.name() + "property is missing");
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

    public CaptureScope getCaptureScope() {
        return captureScope;
    }

    public Optional<String> getCaptureTarget() {
        return Optional.ofNullable(captureTarget);
    }

    public boolean isOffsetInvalidationAllowed() {
        return offsetInvalidationAllowed;
    }

    public ReplicaSet getReplicaSet() {
        return replicaSet;
    }

    public int getCursorMaxAwaitTime() {
        return cursorMaxAwaitTimeMs;
    }

    public CursorPipelineOrder getCursorPipelineOrder() {
        return cursorPipelineOrder;
    }

    public OversizeHandlingMode getOversizeHandlingMode() {
        return oversizeHandlingMode;
    }

    public int getOversizeSkipThreshold() {
        return oversizeSkipThreshold;
    }

    public FiltersMatchMode getFiltersMatchMode() {
        return filtersMatchMode;
    }

    @Override
    public int getSnapshotMaxThreads() {
        return snapshotMaxThreads;
    }

    @Override
    protected SourceInfoStructMaker<? extends AbstractSourceInfo> getSourceInfoStructMaker(Version version) {
        return getSourceInfoStructMaker(SOURCE_INFO_STRUCT_MAKER, Module.name(), Module.version(), this);
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
        return config.getInteger(SNAPSHOT_MAX_THREADS);
    }

    private static ReplicaSet resolveReplicaSet(Configuration config) {
        var connectionString = config.getString(MongoDbConnectorConfig.TASK_CONNECTION_STRING);
        return new ReplicaSet(connectionString);
    }

    @Override
    public Optional<String[]> parseSignallingMessage(Struct value) {
        final String after = value.getString(Envelope.FieldName.AFTER);
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
