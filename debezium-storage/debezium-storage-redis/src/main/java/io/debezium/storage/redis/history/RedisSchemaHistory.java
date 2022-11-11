/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.redis.history;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

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

    private final DocumentWriter writer = DocumentWriter.defaultWriter();
    private final DocumentReader reader = DocumentReader.defaultReader();
    private final AtomicBoolean running = new AtomicBoolean();

    private RedisClient client;

    private RedisSchemaHistoryConfig config;

    void connect() {
        RedisConnection redisConnection = new RedisConnection(config.getAddress(), config.getUser(), config.getPassword(), config.getConnectionTimeout(),
                config.getSocketTimeout(), config.isSslEnabled());
        client = redisConnection.getRedisClient(RedisConnection.DEBEZIUM_SCHEMA_HISTORY, config.isWaitEnabled(), config.getWaitTimeout(),
                config.isWaitRetryEnabled(), config.getWaitRetryDelay());
    }

    @Override
    public void configure(Configuration config, HistoryRecordComparator comparator, SchemaHistoryListener listener, boolean useCatalogBeforeSchema) {
        this.config = new RedisSchemaHistoryConfig(config);
        this.initialRetryDelay = Duration.ofMillis(this.config.getInitialRetryDelay());
        this.maxRetryDelay = Duration.ofMillis(this.config.getMaxRetryDelay());
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
                client.xadd(config.getRedisKeyName(), Collections.singletonMap("schema", line));
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
                entries = client.xrange(config.getRedisKeyName());
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
        if (client != null && client.xlen(config.getRedisKeyName()) > 0) {
            return true;
        }
        else {
            return false;
        }
    }
}
