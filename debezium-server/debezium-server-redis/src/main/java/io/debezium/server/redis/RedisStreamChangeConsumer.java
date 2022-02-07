/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
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
import io.debezium.server.CustomConsumerBuilder;
import io.debezium.util.DelayStrategy;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

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

    private DelayStrategy delayStrategy;
    private HostAndPort address;
    private Optional<String> user;
    private Optional<String> password;

    @ConfigProperty(name = PROP_PREFIX + "retry.initial.delay.ms", defaultValue = "300")
    Integer initialRetryDelay;

    @ConfigProperty(name = PROP_PREFIX + "retry.max.delay.ms", defaultValue = "10000")
    Integer maxRetryDelay;

    @ConfigProperty(name = PROP_PREFIX + "null.key", defaultValue = "default")
    String nullKey;

    @ConfigProperty(name = PROP_PREFIX + "null.value", defaultValue = "default")
    String nullValue;

    private Jedis client = null;

    @Inject
    @CustomConsumerBuilder
    Instance<Jedis> customClient;

    @PostConstruct
    void connect() {
        delayStrategy = DelayStrategy.exponential(initialRetryDelay, maxRetryDelay);

        if (customClient.isResolvable()) {
            client = customClient.get();
            try {
                client.ping();
                LOGGER.info("Obtained custom configured Jedis '{}'", client);
                return;
            }
            catch (Exception e) {
                LOGGER.warn("Invalid custom configured Jedis '{}'", client);
            }
        }

        final Config config = ConfigProvider.getConfig();
        address = HostAndPort.from(config.getValue(PROP_ADDRESS, String.class));
        user = config.getOptionalValue(PROP_USER, String.class);
        password = config.getOptionalValue(PROP_PASSWORD, String.class);

        client = new Jedis(address);
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

        LOGGER.info("Using default Jedis '{}'", client);
    }

    @PreDestroy
    void close() {
        try {
            client.close();
        }
        catch (Exception e) {
            LOGGER.warn("Exception while closing Jedis: {}", client, e);
        }
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records,
                            RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {

        for (ChangeEvent<Object, Object> record : records) {
            LOGGER.trace("Received event '{}'", record);

            String destination = streamNameMapper.map(record.destination());
            String key = (record.key() != null) ? getString(record.key()) : nullKey;
            String value = (record.value() != null) ? getString(record.value()) : nullValue;
            boolean completedSuccessfully = false;

            // As long as we failed to add the current record to the stream, we should retry if the reason was either a connection error or OOM in Redis.
            while (!completedSuccessfully) {
                try {
                    // Add the record to the destination stream
                    client.xadd(destination, null, Collections.singletonMap(key, value));
                    completedSuccessfully = true;
                }
                catch (JedisConnectionException jce) {
                    // Try to reconnect
                    try {
                        connect();
                    }
                    catch (Exception e) {
                        LOGGER.error("Can't connect to Redis", e);
                    }
                }
                catch (Exception e) {
                    // When Redis reaches its max memory limitation, a JedisDataException will be thrown with this message.
                    // In this case, we will retry adding this record to the stream, assuming some memory will be freed eventually as result
                    // of evicting elements from the stream by the target DB.
                    if (e.getMessage().equals("OOM command not allowed when used memory > 'maxmemory'.")) {
                        LOGGER.error("Redis runs OOM", e);
                    }
                    // When Redis is starting, a JedisDataException will be thrown with this message.
                    // We will retry communicating with the target DB as once of the Redis is available, this message will be gone.
                    else if (e.getMessage().equals("LOADING Redis is loading the dataset in memory")) {
                        LOGGER.error("Redis is starting", e);
                    }
                    // In case of unexpected runtime error, throw a DebeziumException which terminates the process
                    else {
                        throw new DebeziumException(e);
                    }
                }

                // Failed to add the record to the stream, retry...
                delayStrategy.sleepWhen(!completedSuccessfully);
            }

            committer.markProcessed(record);
        }

        committer.markBatchFinished();
    }
}
