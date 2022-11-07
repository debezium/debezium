/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.redis.history;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.history.AbstractSchemaHistory;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.SchemaHistoryException;
import io.debezium.relational.history.SchemaHistoryListener;
import io.debezium.storage.redis.RedisClient;
import io.debezium.storage.redis.RedisClientConnectionException;
import io.debezium.storage.redis.RedisConnection;
import io.debezium.util.Collect;
import io.debezium.util.DelayStrategy;
import io.debezium.util.Loggings;

/**
 * A {@link SchemaHistory} implementation that stores the schema history in Redis.
 *
 */
@ThreadSafe
public class RedisSchemaHistory extends AbstractSchemaHistory {

    private static final String CONFIGURATION_FIELD_PREFIX_STRING = SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + "redis.";

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisSchemaHistory.class);

    public static final Field PROP_ADDRESS = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "address")
            .withDescription("The Redis url that will be used to access the database schema history");

    public static final Field PROP_SSL_ENABLED = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "ssl.enabled")
            .withDescription("Use SSL for Redis connection")
            .withDefault("false");

    public static final Field PROP_USER = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "user")
            .withDescription("The Redis url that will be used to access the database schema history");

    public static final Field PROP_PASSWORD = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "password")
            .withDescription("The Redis url that will be used to access the database schema history");

    public static final Field PROP_KEY = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "key")
            .withDescription("The Redis key that will be used to store the database schema history")
            .withDefault("metadata:debezium:schema_history");

    public static final Integer DEFAULT_RETRY_INITIAL_DELAY = 300;
    public static final Field PROP_RETRY_INITIAL_DELAY = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "retry.initial.delay.ms")
            .withDescription("Initial retry delay (in ms)")
            .withDefault(DEFAULT_RETRY_INITIAL_DELAY);

    public static final Integer DEFAULT_RETRY_MAX_DELAY = 10000;
    public static final Field PROP_RETRY_MAX_DELAY = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "retry.max.delay.ms")
            .withDescription("Maximum retry delay (in ms)")
            .withDefault(DEFAULT_RETRY_MAX_DELAY);

    public static final Integer DEFAULT_CONNECTION_TIMEOUT = 2000;
    public static final Field PROP_CONNECTION_TIMEOUT = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "connection.timeout.ms")
            .withDescription("Connection timeout (in ms)")
            .withDefault(DEFAULT_CONNECTION_TIMEOUT);

    public static final Integer DEFAULT_SOCKET_TIMEOUT = 2000;
    public static final Field PROP_SOCKET_TIMEOUT = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "socket.timeout.ms")
            .withDescription("Socket timeout (in ms)")
            .withDefault(DEFAULT_SOCKET_TIMEOUT);

    private static final Field PROP_WAIT_ENABLED = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "wait.enabled")
            .withDescription(
                    "Enables wait for replica. In case Redis is configured with a replica shard, this allows to verify that the data has been written to the replica.")
            .withDefault(false);
    private static final Field PROP_WAIT_TIMEOUT = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "wait.timeout.ms")
            .withDescription("Timeout when wait for replica")
            .withDefault(1000L);
    private static final Field PROP_WAIT_RETRY_ENABLED = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "wait.retry.enabled")
            .withDescription("Enables retry on wait for replica failure")
            .withDefault(false);
    private static final Field PROP_WAIT_RETRY_DELAY = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "wait.retry.delay.ms")
            .withDescription("Delay of retry on wait for replica failure")
            .withDefault(1000L);

    Duration initialRetryDelay;
    Duration maxRetryDelay;

    public static Collection<Field> ALL_FIELDS = Collect.arrayListOf(PROP_ADDRESS, PROP_USER, PROP_PASSWORD, PROP_KEY);

    private final DocumentWriter writer = DocumentWriter.defaultWriter();
    private final DocumentReader reader = DocumentReader.defaultReader();
    private final AtomicBoolean running = new AtomicBoolean();
    private Configuration config;
    private String redisKeyName;
    private String address;
    private String user;
    private String password;
    private boolean sslEnabled;
    private Integer connectionTimeout;
    private Integer socketTimeout;

    private boolean waitEnabled;
    private long waitTimeout;
    private boolean waitRetryEnabled;
    private long waitRetryDelay;

    private RedisClient client;

    void connect() {
        RedisConnection redisConnection = new RedisConnection(this.address, this.user, this.password, this.connectionTimeout, this.socketTimeout, this.sslEnabled);
        client = redisConnection.getRedisClient(RedisConnection.DEBEZIUM_SCHEMA_HISTORY, waitEnabled, waitTimeout, waitRetryEnabled, waitRetryDelay);
    }

    @Override
    public void configure(Configuration config, HistoryRecordComparator comparator, SchemaHistoryListener listener, boolean useCatalogBeforeSchema) {
        if (!config.validateAndRecord(ALL_FIELDS, LOGGER::error)) {
            throw new ConnectException(
                    "Error configuring an instance of " + getClass().getSimpleName() + "; check the logs for details");
        }
        this.config = config;
        // fetch the properties
        this.address = this.config.getString(PROP_ADDRESS.name());
        this.user = this.config.getString(PROP_USER.name());
        this.password = this.config.getString(PROP_PASSWORD.name());
        this.sslEnabled = Boolean.parseBoolean(this.config.getString(PROP_SSL_ENABLED.name()));
        this.redisKeyName = this.config.getString(PROP_KEY);
        LOGGER.info("rediskeyname:" + this.redisKeyName);
        // load retry settings
        this.initialRetryDelay = Duration.ofMillis(this.config.getInteger(PROP_RETRY_INITIAL_DELAY));
        this.maxRetryDelay = Duration.ofMillis(this.config.getInteger(PROP_RETRY_MAX_DELAY));
        // load connection timeout settings
        this.connectionTimeout = this.config.getInteger(PROP_CONNECTION_TIMEOUT);
        this.socketTimeout = this.config.getInteger(PROP_SOCKET_TIMEOUT);

        this.waitEnabled = this.config.getBoolean(PROP_WAIT_ENABLED);
        this.waitTimeout = this.config.getLong(PROP_WAIT_TIMEOUT);
        this.waitRetryEnabled = this.config.getBoolean(PROP_WAIT_RETRY_ENABLED);
        this.waitRetryDelay = this.config.getLong(PROP_WAIT_RETRY_DELAY);

        super.configure(config, comparator, listener, useCatalogBeforeSchema);
    }

    @Override
    public synchronized void start() {
        super.start();
        LOGGER.info("Starting RedisSchemaHistory");
        this.connect();
    }

    @Override
    protected void storeRecord(HistoryRecord record) throws SchemaHistoryException {
        if (record == null) {
            return;
        }
        String line;
        try {
            line = writer.write(record.document());
        }
        catch (IOException e) {
            Loggings.logErrorAndTraceRecord(LOGGER, record, "Failed to convert record to string", e);
            throw new SchemaHistoryException("Unable to write database schema history record");
        }

        DelayStrategy delayStrategy = DelayStrategy.exponential(initialRetryDelay, maxRetryDelay);
        boolean completedSuccessfully = false;

        // loop and retry until successful
        while (!completedSuccessfully) {
            try {
                if (client == null) {
                    this.connect();
                }

                // write the entry to Redis
                client.xadd(this.redisKeyName, Collections.singletonMap("schema", line));
                LOGGER.trace("Record written to database schema history in Redis: " + line);
                completedSuccessfully = true;
            }
            catch (RedisClientConnectionException e) {
                LOGGER.warn("Attempting to reconnect to Redis");
                this.connect();
            }
            catch (Exception e) {
                LOGGER.warn("Writing to database schema history stream failed", e);
                LOGGER.warn("Will retry");
            }
            if (!completedSuccessfully) {
                // Failed to execute the transaction, retry...
                delayStrategy.sleepWhen(!completedSuccessfully);
            }

        }
    }

    @Override
    public void stop() {
        running.set(false);
        if (client != null) {
            client.disconnect();
        }
        super.stop();
    }

    @Override
    protected synchronized void recoverRecords(Consumer<HistoryRecord> records) {
        DelayStrategy delayStrategy = DelayStrategy.exponential(initialRetryDelay, maxRetryDelay);
        boolean completedSuccessfully = false;
        List<Map<String, String>> entries = new ArrayList<>();

        // loop and retry until successful
        while (!completedSuccessfully) {
            try {
                if (client == null) {
                    this.connect();
                }

                // read the entries from Redis
                entries = client.xrange(this.redisKeyName);
                completedSuccessfully = true;
            }
            catch (RedisClientConnectionException e) {
                LOGGER.warn("Attempting to reconnect to Redis");
                this.connect();
            }
            catch (Exception e) {
                LOGGER.warn("Reading from database schema history stream failed with " + e);
                LOGGER.warn("Will retry");
            }
            if (!completedSuccessfully) {
                // Failed to execute the transaction, retry...
                delayStrategy.sleepWhen(!completedSuccessfully);
            }

        }

        for (Map<String, String> item : entries) {
            try {
                records.accept(new HistoryRecord(reader.read(item.get("schema"))));
            }
            catch (IOException e) {
                LOGGER.error("Failed to convert record to string: {}", item, e);
                return;
            }
        }

    }

    @Override
    public boolean storageExists() {
        return true;
    }

    @Override
    public boolean exists() {
        // check if the stream is not empty
        if (client != null && client.xlen(this.redisKeyName) > 0) {
            return true;
        }
        else {
            return false;
        }
    }
}
