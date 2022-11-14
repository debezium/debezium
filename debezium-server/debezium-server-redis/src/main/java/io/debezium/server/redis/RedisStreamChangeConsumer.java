/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.inject.Named;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.storage.redis.RedisClient;
import io.debezium.storage.redis.RedisClientConnectionException;
import io.debezium.storage.redis.RedisConnection;
import io.debezium.util.DelayStrategy;
import io.debezium.util.IoUtil;

/**
 * Implementation of the consumer that delivers the messages into Redis (stream) destination.
 *
 * @author M Sazzadul Hoque
 * @author Yossi Shirizli
 */
@Named("redis")
@Dependent
public class RedisStreamChangeConsumer extends BaseChangeConsumer
        implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisStreamChangeConsumer.class);

    private static final String DEBEZIUM_REDIS_SINK_CLIENT_NAME = "debezium:redis:sink";

    private static final String PROP_PREFIX = "debezium.sink.redis.";
    private static final String PROP_ADDRESS = PROP_PREFIX + "address";
    private static final String PROP_USER = PROP_PREFIX + "user";
    private static final String PROP_PASSWORD = PROP_PREFIX + "password";
    private static final String PROP_CONNECTION_TIMEOUT = PROP_PREFIX + "connection.timeout.ms";
    private static final String PROP_SOCKET_TIMEOUT = PROP_PREFIX + "socket.timeout.ms";
    private static final String PROP_MESSAGE_FORMAT = PROP_PREFIX + "message.format";
    private static final String PROP_WAIT_ENABLED = PROP_PREFIX + "wait.enabled";
    private static final String PROP_WAIT_TIMEOUT = PROP_PREFIX + "wait.timeout.ms";
    private static final String PROP_WAIT_RETRY_ENABLED = PROP_PREFIX + "wait.retry.enabled";
    private static final String PROP_WAIT_RETRY_DELAY = PROP_PREFIX + "wait.retry.delay.ms";

    private static final int DEFAULT_MEMORY_THRESHOLD_PERCENTAGE = 85;
    private static final String PROP_MEMORY_THRESHOLD_PERCENTAGE = PROP_PREFIX + "memory.threshold.percentage";
    private static final String INFO_MEMORY = "memory";
    private static final String INFO_MEMORY_SECTION_MAXMEMORY = "maxmemory";
    private static final String INFO_MEMORY_SECTION_USEDMEMORY = "used_memory";

    private static final String MESSAGE_FORMAT_COMPACT = "compact";
    private static final String MESSAGE_FORMAT_EXTENDED = "extended";
    private static final String EXTENDED_MESSAGE_KEY_KEY = "key";
    private static final String EXTENDED_MESSAGE_VALUE_KEY = "value";

    private String address;
    private String user;
    private String password;
    private Integer connectionTimeout;
    private Integer socketTimeout;

    @ConfigProperty(name = PROP_PREFIX + "ssl.enabled", defaultValue = "false")
    boolean sslEnabled;

    @ConfigProperty(name = PROP_PREFIX + "batch.size", defaultValue = "500")
    Integer batchSize;

    @ConfigProperty(name = PROP_PREFIX + "retry.initial.delay.ms", defaultValue = "300")
    Integer initialRetryDelay;

    @ConfigProperty(name = PROP_PREFIX + "retry.max.delay.ms", defaultValue = "10000")
    Integer maxRetryDelay;

    @ConfigProperty(name = PROP_PREFIX + "null.key", defaultValue = "default")
    String nullKey;

    @ConfigProperty(name = PROP_PREFIX + "null.value", defaultValue = "default")
    String nullValue;

    private RedisClient client;

    private BiFunction<String, String, Map<String, String>> recordMapFunction;

    private Supplier<Boolean> isMemoryOk;

    @PostConstruct
    void connect() {
        final Config config = ConfigProvider.getConfig();
        address = config.getValue(PROP_ADDRESS, String.class);
        user = config.getOptionalValue(PROP_USER, String.class).orElse(null);
        password = config.getOptionalValue(PROP_PASSWORD, String.class).orElse(null);
        connectionTimeout = config.getOptionalValue(PROP_CONNECTION_TIMEOUT, Integer.class).orElse(2000);
        socketTimeout = config.getOptionalValue(PROP_SOCKET_TIMEOUT, Integer.class).orElse(2000);

        String messageFormat = config.getOptionalValue(PROP_MESSAGE_FORMAT, String.class).orElse(MESSAGE_FORMAT_COMPACT);
        LOGGER.info("Property {}={}", PROP_MESSAGE_FORMAT, messageFormat);
        if (MESSAGE_FORMAT_EXTENDED.equals(messageFormat)) {
            recordMapFunction = (key, value) -> {
                Map<String, String> recordMap = new LinkedHashMap<>(2);
                recordMap.put(EXTENDED_MESSAGE_KEY_KEY, key);
                recordMap.put(EXTENDED_MESSAGE_VALUE_KEY, value);
                return recordMap;
            };
        }
        else if (MESSAGE_FORMAT_COMPACT.equals(messageFormat)) {
            recordMapFunction = Collections::singletonMap;
        }
        else {
            throw new DebeziumException(
                    String.format("Property %s expects value one of '%s' or '%s'", PROP_MESSAGE_FORMAT, MESSAGE_FORMAT_EXTENDED, MESSAGE_FORMAT_COMPACT));
        }

        int memoryThreshold = config.getOptionalValue(PROP_MEMORY_THRESHOLD_PERCENTAGE, Integer.class).orElse(DEFAULT_MEMORY_THRESHOLD_PERCENTAGE);
        if (memoryThreshold < 0 || memoryThreshold > 100) {
            throw new DebeziumException(String.format("Property %s should be between 0 and 100", PROP_MEMORY_THRESHOLD_PERCENTAGE));
        }
        isMemoryOk = memoryThreshold > 0 ? () -> isMemoryOk(memoryThreshold) : () -> true;

        boolean waitEnabled = config.getOptionalValue(PROP_WAIT_ENABLED, Boolean.class).orElse(false);
        long waitTimeout = config.getOptionalValue(PROP_WAIT_TIMEOUT, Long.class).orElse(1000L);
        boolean waitRetryEnabled = config.getOptionalValue(PROP_WAIT_RETRY_ENABLED, Boolean.class).orElse(false);
        long waitRetryDelay = config.getOptionalValue(PROP_WAIT_RETRY_DELAY, Long.class).orElse(1000L);

        RedisConnection redisConnection = new RedisConnection(address, user, password, connectionTimeout, socketTimeout, sslEnabled);
        client = redisConnection.getRedisClient(DEBEZIUM_REDIS_SINK_CLIENT_NAME, waitEnabled, waitTimeout, waitRetryEnabled, waitRetryDelay);
    }

    @PreDestroy
    void close() {
        try {
            if (client != null) {
                client.close();
            }
        }
        catch (Exception e) {
            LOGGER.warn("Exception while closing Jedis: {}", client, e);
        }
        finally {
            client = null;
        }
    }

    /**
    * Split collection to batches by batch size using a stream
    */
    private <T> Stream<List<T>> batches(List<T> source, int length) {
        if (source.isEmpty()) {
            return Stream.empty();
        }

        int size = source.size();
        int fullChunks = (size - 1) / length;

        return IntStream.range(0, fullChunks + 1).mapToObj(
                n -> source.subList(n * length, n == fullChunks ? size : (n + 1) * length));
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records,
                            RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        DelayStrategy delayStrategy = DelayStrategy.exponential(Duration.ofMillis(initialRetryDelay), Duration.ofMillis(maxRetryDelay));

        LOGGER.trace("Handling a batch of {} records", records.size());
        batches(records, batchSize).forEach(batch -> {
            boolean completedSuccessfully = false;

            // Clone the batch and remove the records that have been successfully processed.
            // Move to the next batch once this list is empty.
            List<ChangeEvent<Object, Object>> clonedBatch = batch.stream().collect(Collectors.toList());

            // As long as we failed to execute the current batch to the stream, we should retry if the reason was either a connection error or OOM in Redis.
            while (!completedSuccessfully) {
                if (client == null) {
                    // Try to reconnect
                    try {
                        connect();
                        continue; // Managed to establish a new connection to Redis, avoid a redundant retry
                    }
                    catch (Exception e) {
                        close();
                        LOGGER.error("Can't connect to Redis", e);
                    }
                }
                else if (canHandleBatch()) {
                    try {
                        LOGGER.trace("Preparing a Redis Pipeline of {} records", clonedBatch.size());

                        List<SimpleEntry<String, Map<String, String>>> recordsMap = new ArrayList<>(clonedBatch.size());
                        for (ChangeEvent<Object, Object> record : clonedBatch) {
                            String destination = streamNameMapper.map(record.destination());
                            String key = (record.key() != null) ? getString(record.key()) : nullKey;
                            String value = (record.value() != null) ? getString(record.value()) : nullValue;
                            Map<String, String> recordMap = recordMapFunction.apply(key, value);
                            recordsMap.add(new SimpleEntry<>(destination, recordMap));
                        }
                        List<String> responses = client.xadd(recordsMap);
                        List<ChangeEvent<Object, Object>> processedRecords = new ArrayList<ChangeEvent<Object, Object>>();
                        int index = 0;
                        int totalOOMResponses = 0;

                        for (String message : responses) {
                            // When Redis reaches its max memory limitation, an OOM error message will be retrieved.
                            // In this case, we will retry execute the failed commands, assuming some memory will be freed eventually as result
                            // of evicting elements from the stream by the target DB.
                            if (message.contains("OOM command not allowed when used memory > 'maxmemory'")) {
                                totalOOMResponses++;
                            }
                            else {
                                // Mark the record as processed
                                ChangeEvent<Object, Object> currentRecord = clonedBatch.get(index);
                                committer.markProcessed(currentRecord);
                                processedRecords.add(currentRecord);
                            }

                            index++;
                        }

                        clonedBatch.removeAll(processedRecords);

                        if (totalOOMResponses > 0) {
                            LOGGER.warn("Redis runs OOM, {} command(s) failed", totalOOMResponses);
                        }

                        if (clonedBatch.size() == 0) {
                            completedSuccessfully = true;
                        }
                    }
                    catch (RedisClientConnectionException jce) {
                        LOGGER.error("Connection error", jce);
                        close();
                    }
                    catch (Exception e) {
                        LOGGER.error("Unexpected Exception", e);
                        throw new DebeziumException(e);
                    }
                }
                else {
                    LOGGER.warn("Stopped consuming records!");
                }

                // Failed to execute the transaction, retry...
                delayStrategy.sleepWhen(!completedSuccessfully);
            }
        });

        // Mark the whole batch as finished once the sub batches completed
        committer.markBatchFinished();
    }

    private boolean canHandleBatch() {
        return isMemoryOk.get();
    }

    private boolean isMemoryOk(int memoryThreshold) {
        String memory = client.info(INFO_MEMORY);
        Map<String, String> infoMemory = new HashMap<>();
        try {
            IoUtil.readLines(new ByteArrayInputStream(memory.getBytes(StandardCharsets.UTF_8)), line -> {
                String[] pair = line.split(":");
                if (pair.length == 2) {
                    infoMemory.put(pair[0], pair[1]);
                }
            });
        }
        catch (IOException e) {
            LOGGER.error("Cannot parse Redis info memory {}", memory, e);
            return true;
        }
        long maxMemory = Long.parseLong(infoMemory.get(INFO_MEMORY_SECTION_MAXMEMORY));
        if (maxMemory > 0) {
            long usedMemory = Long.parseLong(infoMemory.get(INFO_MEMORY_SECTION_USEDMEMORY));
            long percentage = 100 * usedMemory / maxMemory;
            if (percentage >= memoryThreshold) {
                LOGGER.warn("Used memory percentage of {}% is higher than configured threshold of {}%", percentage, memoryThreshold);
                return false;
            }
        }
        return true;
    }
}