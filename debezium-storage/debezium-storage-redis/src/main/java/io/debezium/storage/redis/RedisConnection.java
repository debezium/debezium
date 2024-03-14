/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.redis;

import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.util.Strings;

import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;

/**
 * Establishes a new connection to Redis
 *
 * @author Yossi Shirizli
 */
public class RedisConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisConnection.class);

    public static final String DEBEZIUM_OFFSETS_CLIENT_NAME = "debezium:offsets";
    public static final String DEBEZIUM_SCHEMA_HISTORY = "debezium:schema_history";
    private static final String HOST_PORT_ERROR = "Invalid host:port format in '<...>.redis.address' property.";

    private String address;
    private int dbIndex;
    private String user;
    private String password;
    private int connectionTimeout;
    private int socketTimeout;
    private boolean sslEnabled;

    /**
     *
     * @param address
     * @param user
     * @param password
     * @param connectionTimeout
     * @param socketTimeout
     * @param sslEnabled
     */
    public RedisConnection(String address, int dbIndex, String user, String password, int connectionTimeout, int socketTimeout, boolean sslEnabled) {
        validateHostPort(address);

        this.address = address;
        this.dbIndex = dbIndex;
        this.user = user;
        this.password = password;
        this.connectionTimeout = connectionTimeout;
        this.socketTimeout = socketTimeout;
        this.sslEnabled = sslEnabled;
    }

    /**
     *
     * @param clientName
     * @param waitEnabled
     * @param waitTimeout
     * @param waitRetry
     * @param waitRetryDelay
     * @return
     * @throws RedisClientConnectionException
     */
    public RedisClient getRedisClient(String clientName, boolean waitEnabled, long waitTimeout, boolean waitRetry, long waitRetryDelay) {
        if (waitEnabled && waitTimeout <= 0) {
            throw new DebeziumException("Redis client wait timeout should be positive");
        }

        HostAndPort address = HostAndPort.from(this.address);

        Jedis client;
        try {
            client = new Jedis(address, DefaultJedisClientConfig.builder().database(this.dbIndex).connectionTimeoutMillis(this.connectionTimeout)
                    .socketTimeoutMillis(this.socketTimeout).ssl(this.sslEnabled).build());

            if (!Strings.isNullOrEmpty(this.user)) {
                client.auth(this.user, this.password);
            }
            else if (!Strings.isNullOrEmpty(this.password)) {
                client.auth(this.password);
            }
            else {
                // make sure that client is connected
                client.ping();
            }

            try {
                client.clientSetname(clientName);
            }
            catch (JedisDataException e) {
                LOGGER.warn("Failed to set client name", e);
            }
        }
        catch (JedisConnectionException e) {
            throw new RedisClientConnectionException(e);
        }

        RedisClient jedisClient = new JedisClient(client);

        // we use 1 for number of replicas as in Redis Enterprise there can be only one replica shard
        RedisClient redisClient = waitEnabled ? new WaitReplicasRedisClient(jedisClient, 1, waitTimeout, waitRetry, waitRetryDelay) : jedisClient;

        LOGGER.info("Using Redis client '{}'", redisClient);

        return redisClient;
    }

    private void validateHostPort(String address) {
        Pattern pattern = Pattern.compile("^[\\w.-]+:\\d{1,5}+$");
        if (!pattern.matcher(address).matches()) {
            throw new DebeziumException(HOST_PORT_ERROR);
        }
    }
}
