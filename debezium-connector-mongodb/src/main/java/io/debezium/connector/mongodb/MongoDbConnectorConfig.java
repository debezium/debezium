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
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.config.Field.ValidationOutput;

/**
 * The configuration properties.
 */
public class MongoDbConnectorConfig extends CommonConnectorConfig {

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

    public static final Field POLL_INTERVAL_SEC = Field.create("mongodb.poll.interval.sec")
                                                       .withDisplayName("Replica membership poll interval (sec)")
                                                       .withType(Type.INT)
                                                       .withWidth(Width.SHORT)
                                                       .withImportance(Importance.MEDIUM)
                                                       .withDefault(30)
                                                       .withValidation(Field::isPositiveInteger)
                                                       .withDescription("Frequency in seconds to look for new, removed, or changed replica sets. Defaults to 30 seconds.");

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

    public static final Field SSL_KEYSTORE = Field.create("mongodb.ssl.keystore")
            .withDisplayName("SSL Keystore")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDescription("Location of the Java keystore file containing an application process's own certificate and private key.");

    public static final Field SSL_KEYSTORE_PASSWORD = Field.create("mongodb.ssl.keystore.password")
            .withDisplayName("SSL Keystore Password")
            .withType(Type.PASSWORD)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDescription("Password to access the private key from the keystore file specified by 'ssl.keystore' configuration property or the 'javax.net.ssl.keyStore' system or JVM property. "
                    + "This password is used to unlock the keystore file (store password), and to decrypt the private key stored in the keystore (key password).");

    public static final Field SSL_TRUSTSTORE = Field.create("mongodb.ssl.truststore")
            .withDisplayName("SSL Truststore")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDescription("Location of the Java truststore file containing the collection of CA certificates trusted by this application process (trust store).");

    public static final Field SSL_TRUSTSTORE_PASSWORD = Field.create("mongodb.ssl.truststore.password")
            .withDisplayName("SSL Truststore Password")
            .withType(Type.PASSWORD)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDescription("Password to unlock the keystore file (store password) specified by 'ssl.trustore' configuration property or the 'javax.net.ssl.trustStore' system or JVM property.");

    public static final Field MAX_COPY_THREADS = Field.create("initial.sync.max.threads")
                                                      .withDisplayName("Maximum number of threads for initial sync")
                                                      .withType(Type.INT)
                                                      .withWidth(Width.SHORT)
                                                      .withImportance(Importance.MEDIUM)
                                                      .withDefault(1)
                                                      .withValidation(Field::isPositiveInteger)
                                                      .withDescription("Maximum number of threads used to perform an intial sync of the collections in a replica set. "
                                                              + "Defaults to 1.");

    public static final Field CONNECT_BACKOFF_INITIAL_DELAY_MS = Field.create("connect.backoff.initial.delay.ms")
                                                                      .withDisplayName("Initial delay before reconnection (ms)")
                                                                      .withType(Type.LONG)
                                                                      .withWidth(Width.SHORT)
                                                                      .withImportance(Importance.MEDIUM)
                                                                      .withDefault(TimeUnit.SECONDS.toMillis(1))
                                                                      .withValidation(Field::isPositiveInteger)
                                                                      .withDescription("The initial delay when trying to reconnect to a primary after a connection cannot be made or when no primary is available. Defaults to 1 second (1000 ms).");

    public static final Field CONNECT_BACKOFF_MAX_DELAY_MS = Field.create("connect.backoff.max.delay.ms")
                                                                  .withDisplayName("Maximum delay before reconnection (ms)")
                                                                  .withType(Type.LONG)
                                                                  .withWidth(Width.SHORT)
                                                                  .withImportance(Importance.MEDIUM)
                                                                  .withDefault(TimeUnit.SECONDS.toMillis(120))
                                                                  .withValidation(Field::isPositiveInteger)
                                                                  .withDescription("The maximum delay when trying to reconnect to a primary after a connection cannot be made or when no primary is available. Defaults to 120 second (120,000 ms).");

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

    protected static final Field TASK_ID = Field.create("mongodb.task.id")
                                                .withDescription("Internal use only")
                                                .withValidation(Field::isInteger)
                                                .withInvisibleRecommender();

    public static Field.Set ALL_FIELDS = Field.setOf(USER, PASSWORD, HOSTS, LOGICAL_NAME,
                                                     SSL_ENABLED, SSL_ALLOW_INVALID_HOSTNAMES,
                                                     MAX_COPY_THREADS, CommonConnectorConfig.MAX_QUEUE_SIZE,
                                                     CommonConnectorConfig.MAX_BATCH_SIZE,
                                                     CommonConnectorConfig.POLL_INTERVAL_MS,
                                                     MAX_FAILED_CONNECTIONS,
                                                     CONNECT_BACKOFF_INITIAL_DELAY_MS,
                                                     CONNECT_BACKOFF_MAX_DELAY_MS,
                                                     COLLECTION_WHITELIST,
                                                     COLLECTION_BLACKLIST,
                                                     FIELD_BLACKLIST,
                                                     FIELD_RENAMES,
                                                     AUTO_DISCOVER_MEMBERS,
                                                     DATABASE_WHITELIST,
                                                     DATABASE_BLACKLIST,
                                                     CommonConnectorConfig.TOMBSTONES_ON_DELETE,
                                                     CommonConnectorConfig.SNAPSHOT_DELAY_MS);

    protected static Field.Set EXPOSED_FIELDS = ALL_FIELDS;

    public MongoDbConnectorConfig(Configuration config) {
        super(config, config.getString(LOGICAL_NAME));
    }

    protected static ConfigDef configDef() {
        ConfigDef config = new ConfigDef();
        Field.group(config, "MongoDB", HOSTS, USER, PASSWORD, LOGICAL_NAME, CONNECT_BACKOFF_INITIAL_DELAY_MS,
                    CONNECT_BACKOFF_MAX_DELAY_MS, MAX_FAILED_CONNECTIONS, AUTO_DISCOVER_MEMBERS,
                    SSL_ENABLED, SSL_ALLOW_INVALID_HOSTNAMES);
        Field.group(config, "Events", DATABASE_WHITELIST, DATABASE_BLACKLIST, COLLECTION_WHITELIST, COLLECTION_BLACKLIST, FIELD_BLACKLIST, FIELD_RENAMES, CommonConnectorConfig.TOMBSTONES_ON_DELETE);
        Field.group(config, "Connector", MAX_COPY_THREADS, CommonConnectorConfig.MAX_QUEUE_SIZE,
                CommonConnectorConfig.MAX_BATCH_SIZE, CommonConnectorConfig.POLL_INTERVAL_MS,
                CommonConnectorConfig.SNAPSHOT_DELAY_MS);
        return config;
    }

    private static int validateHosts(Configuration config, Field field, ValidationOutput problems) {
        String hosts = config.getString(field);
        if (hosts == null) {
            problems.accept(field, hosts, "Host specification is required");
            return 1;
        }
        int count = 0;
        if (ReplicaSets.parse(hosts) == null) {
            problems.accept(field, hosts, "Invalid host specification");
            ++count;
        }
        return count;
    }

    private static int validateCollectionBlacklist(Configuration config, Field field, ValidationOutput problems) {
        String whitelist = config.getString(COLLECTION_WHITELIST);
        String blacklist = config.getString(COLLECTION_BLACKLIST);
        if (whitelist != null && blacklist != null) {
            problems.accept(COLLECTION_BLACKLIST, blacklist, "Whitelist is already specified");
            return 1;
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
}
