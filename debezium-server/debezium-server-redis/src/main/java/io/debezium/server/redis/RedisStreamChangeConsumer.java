/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
import io.debezium.util.DelayStrategy;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;

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

    private static final String PROP_PREFIX = "debezium.sink.redis.";
    private static final String PROP_ADDRESS = PROP_PREFIX + "address";
    private static final String PROP_USER = PROP_PREFIX + "user";
    private static final String PROP_PASSWORD = PROP_PREFIX + "password";
    private static final String PROP_CONNECTION_TIMEOUT = PROP_PREFIX + "connection.timeout.ms";
    private static final String PROP_SOCKET_TIMEOUT = PROP_PREFIX + "socket.timeout.ms";

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

    private Jedis client = null;

    @PostConstruct
    void connect() {
        final Config config = ConfigProvider.getConfig();
        address = config.getValue(PROP_ADDRESS, String.class);
        user = config.getOptionalValue(PROP_USER, String.class).orElse(null);
        password = config.getOptionalValue(PROP_PASSWORD, String.class).orElse(null);
        connectionTimeout = config.getOptionalValue(PROP_CONNECTION_TIMEOUT, Integer.class).orElse(2000);
        socketTimeout = config.getOptionalValue(PROP_SOCKET_TIMEOUT, Integer.class).orElse(2000);

        RedisConnection redisConnection = new RedisConnection(address, user, password, connectionTimeout, socketTimeout, sslEnabled);
        client = redisConnection.getRedisClient(RedisConnection.DEBEZIUM_REDIS_SINK_CLIENT_NAME);
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
                else {
                    Pipeline pipeline;
                    try {
                        LOGGER.trace("Preparing a Redis Pipeline of {} records", clonedBatch.size());

                        // Make sure the connection is still alive before creating the pipeline
                        // to reduce the chance of ending up with duplicate records
                        client.ping();
                        pipeline = client.pipelined();

                        // Add the batch records to the stream(s) via Pipeline
                        for (ChangeEvent<Object, Object> record : clonedBatch) {
                            String destination = streamNameMapper.map(record.destination());
                            String key = (record.key() != null) ? getString(record.key()) : nullKey;
                            String value = (record.value() != null) ? getString(record.value()) : nullValue;

                            // Add the record to the destination stream
                            pipeline.xadd(destination, StreamEntryID.NEW_ENTRY, Collections.singletonMap(key, value));
                        }

                        // Sync the pipeline in Redis and parse the responses (response per command with the same order)
                        List<Object> responses = pipeline.syncAndReturnAll();
                        List<ChangeEvent<Object, Object>> processedRecords = new ArrayList<ChangeEvent<Object, Object>>();
                        int index = 0;
                        int totalOOMResponses = 0;

                        for (Object response : responses) {
                            String message = response.toString();
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
                    catch (JedisConnectionException jce) {
                        LOGGER.error("Connection error", jce);
                        close();
                    }
                    catch (JedisDataException jde) {
                        // When Redis is starting, a JedisDataException will be thrown with this message.
                        // We will retry communicating with the target DB as once of the Redis is available, this message will be gone.
                        if (jde.getMessage().equals("LOADING Redis is loading the dataset in memory")) {
                            LOGGER.error("Redis is starting", jde);
                        }
                        else {
                            LOGGER.error("Unexpected JedisDataException", jde);
                            throw new DebeziumException(jde);
                        }
                    }
                    catch (Exception e) {
                        LOGGER.error("Unexpected Exception", e);
                        throw new DebeziumException(e);
                    }
                }

                // Failed to execute the transaction, retry...
                delayStrategy.sleepWhen(!completedSuccessfully);
            }
        });

        // Mark the whole batch as finished once the sub batches completed
        committer.markBatchFinished();
    }
}