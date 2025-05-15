/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.redis.history;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.config.Configuration;
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
import io.debezium.util.DelayStrategy;
import io.debezium.util.Loggings;

/**
 * A {@link SchemaHistory} implementation that stores the schema history in Redis.
 *
 */
@ThreadSafe
public class RedisSchemaHistory extends AbstractSchemaHistory {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisSchemaHistory.class);

    private Duration initialRetryDelay;
    private Duration maxRetryDelay;
    private Integer maxRetryCount;

    private final DocumentWriter writer = DocumentWriter.defaultWriter();
    private final DocumentReader reader = DocumentReader.defaultReader();
    private final AtomicBoolean running = new AtomicBoolean();

    private RedisClient client;

    private RedisSchemaHistoryConfig config;

    void connect() {
        RedisConnection redisConnection = new RedisConnection(config.getAddress(), config.getDbIndex(), config.getUser(), config.getPassword(),
                config.getConnectionTimeout(), config.getSocketTimeout(), config.isSslEnabled(), config.isHostnameVerificationEnabled());
        client = redisConnection.getRedisClient(RedisConnection.DEBEZIUM_SCHEMA_HISTORY, config.isWaitEnabled(), config.getWaitTimeout(),
                config.isWaitRetryEnabled(), config.getWaitRetryDelay());
    }

    @Override
    public void configure(Configuration config, HistoryRecordComparator comparator, SchemaHistoryListener listener, boolean useCatalogBeforeSchema) {
        this.config = new RedisSchemaHistoryConfig(config);
        this.initialRetryDelay = Duration.ofMillis(this.config.getInitialRetryDelay());
        this.maxRetryDelay = Duration.ofMillis(this.config.getMaxRetryDelay());
        this.maxRetryCount = this.config.getMaxRetryCount();
        super.configure(config, comparator, listener, useCatalogBeforeSchema);
    }

    @Override
    public synchronized void start() {
        super.start();
        LOGGER.info("Starting RedisSchemaHistory");
        doWithRetry(() -> true, "Connection to Redis");
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

        doWithRetry(() -> {
            // write the entry to Redis
            client.xadd(config.getRedisKeyName(), Collections.singletonMap("schema", line));
            LOGGER.trace("Record written to database schema history in Redis: " + line);
            return true;
        }, "Writing to database schema history stream");
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
        // read the entries from Redis
        final List<Map<String, String>> entries = doWithRetry(() -> client.xrange(config.getRedisKeyName()),
                "Writing to database schema history stream");

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
        return doWithRetry(() -> (client != null) && (client.xlen(config.getRedisKeyName()) > 0), "Check if previous record exists");
    }

    private <T> T doWithRetry(Supplier<T> action, String description) {
        final var delayStrategy = DelayStrategy.exponential(initialRetryDelay, maxRetryDelay);

        // loop and retry until successful or maximum attempts reached
        for (int i = 1; i <= maxRetryCount; i++) {
            try {
                if (client == null) {
                    this.connect();
                }

                return action.get();
            }
            catch (RedisClientConnectionException e) {
                LOGGER.warn("Connection to Redis failed, will try to reconnect [attempt {} of {}]", i, maxRetryCount);
                try {
                    if (client != null) {
                        client.disconnect();
                    }
                }
                catch (Exception eDisconnect) {
                    LOGGER.info("Exception while disconnecting", eDisconnect);
                }
                client = null;
            }
            catch (Exception e) {
                LOGGER.warn(description + " failed, will retry", e);
            }
            // Failed to execute the operation, retry...
            delayStrategy.sleepWhen(true);
        }

        throw new SchemaHistoryException(String.format("Failed to connect to Redis after %d attempts.", maxRetryCount));
    }
}
