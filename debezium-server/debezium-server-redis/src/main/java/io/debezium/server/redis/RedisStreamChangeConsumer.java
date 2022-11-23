/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import static io.debezium.server.redis.RedisStreamChangeConsumerConfig.MESSAGE_FORMAT_COMPACT;
import static io.debezium.server.redis.RedisStreamChangeConsumerConfig.MESSAGE_FORMAT_EXTENDED;

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

import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
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

    private static final String EXTENDED_MESSAGE_KEY_KEY = "key";
    private static final String EXTENDED_MESSAGE_VALUE_KEY = "value";

    private static final String INFO_MEMORY = "memory";
    private static final String INFO_MEMORY_SECTION_MAXMEMORY = "maxmemory";
    private static final String INFO_MEMORY_SECTION_USEDMEMORY = "used_memory";

    private RedisClient client;

    private BiFunction<String, String, Map<String, String>> recordMapFunction;

    private Supplier<Boolean> isMemoryOk;

    private RedisStreamChangeConsumerConfig config;

    @PostConstruct
    void connect() {
        Configuration configuration = Configuration.from(getConfigSubset(ConfigProvider.getConfig(), ""));
        config = new RedisStreamChangeConsumerConfig(configuration);
        String messageFormat = config.getMessageFormat();
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

        int memoryThreshold = config.getMemoryThreshold();
        isMemoryOk = memoryThreshold > 0 ? () -> isMemoryOk(memoryThreshold) : () -> true;

        RedisConnection redisConnection = new RedisConnection(config.getAddress(), config.getUser(), config.getPassword(), config.getConnectionTimeout(),
                config.getSocketTimeout(), config.isSslEnabled());
        client = redisConnection.getRedisClient(DEBEZIUM_REDIS_SINK_CLIENT_NAME, config.isWaitEnabled(), config.getWaitTimeout(), config.isWaitRetryEnabled(),
                config.getWaitRetryDelay());
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
        DelayStrategy delayStrategy = DelayStrategy.exponential(Duration.ofMillis(config.getInitialRetryDelay()), Duration.ofMillis(config.getMaxRetryDelay()));

        LOGGER.trace("Handling a batch of {} records", records.size());
        batches(records, config.getBatchSize()).forEach(batch -> {
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
                            String key = (record.key() != null) ? getString(record.key()) : config.getNullKey();
                            String value = (record.value() != null) ? getString(record.value()) : config.getNullValue();
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