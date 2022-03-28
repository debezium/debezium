/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
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

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;

/**
 * Implementation of the consumer that delivers the messages into Redis (stream) destination.
 *
 * @author M Sazzadul Hoque
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

    private HostAndPort address;
    private Optional<String> user;
    private Optional<String> password;

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
        address = HostAndPort.from(config.getValue(PROP_ADDRESS, String.class));
        user = config.getOptionalValue(PROP_USER, String.class);
        password = config.getOptionalValue(PROP_PASSWORD, String.class);

        client = new Jedis(address.getHost(), address.getPort(), sslEnabled);
        if (user.isPresent()) {
            client.auth(user.get(), password.get());
        }
        else if (password.isPresent()) {
            client.auth(password.get());
        }
        else {
            // make sure that client is connected
            client.ping();
        }

        LOGGER.info("Using Jedis '{}'", client);
    }

    @PreDestroy
    void close() {
        try {
            client.close();
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
        DelayStrategy delayStrategy = DelayStrategy.exponential(initialRetryDelay, maxRetryDelay);

        LOGGER.trace("Handling a batch of {} records", records.size());
        batches(records, batchSize).forEach(batch -> {
            boolean completedSuccessfully = false;

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
                    Transaction transaction;
                    try {
                        LOGGER.trace("Preparing a Redis Transaction of {} records", batch.size());
                        transaction = client.multi();

                        // Add the batch records to the stream(s) via Transaction
                        for (ChangeEvent<Object, Object> record : batch) {
                            String destination = streamNameMapper.map(record.destination());
                            String key = (record.key() != null) ? getString(record.key()) : nullKey;
                            String value = (record.value() != null) ? getString(record.value()) : nullValue;

                            // Add the record to the destination stream
                            transaction.xadd(destination, StreamEntryID.NEW_ENTRY, Collections.singletonMap(key, value));
                        }

                        // Execute the transaction in Redis
                        transaction.exec();

                        // Mark all the batch records as processed only when the transaction succeeds
                        for (ChangeEvent<Object, Object> record : batch) {
                            committer.markProcessed(record);
                        }
                        completedSuccessfully = true;
                    }
                    catch (JedisConnectionException jce) {
                        close();
                    }
                    catch (JedisDataException jde) {
                        // When Redis reaches its max memory limitation, a JedisDataException will be thrown with one of the messages listed below.
                        // In this case, we will retry execute the batch, assuming some memory will be freed eventually as result
                        // of evicting elements from the stream by the target DB.
                        if (jde.getMessage().equals("EXECABORT Transaction discarded because of: OOM command not allowed when used memory > 'maxmemory'.")) {
                            LOGGER.error("Redis runs OOM", jde);
                        }
                        else if (jde.getMessage().startsWith("EXECABORT")) {
                            LOGGER.error("Redis transaction error", jde);
                        }
                        // When Redis is starting, a JedisDataException will be thrown with this message.
                        // We will retry communicating with the target DB as once of the Redis is available, this message will be gone.
                        else if (jde.getMessage().equals("LOADING Redis is loading the dataset in memory")) {
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
