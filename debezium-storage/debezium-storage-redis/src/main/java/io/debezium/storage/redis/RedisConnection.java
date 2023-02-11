/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;

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

    private String address;
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
    public RedisConnection(String address, String user, String password, int connectionTimeout, int socketTimeout, boolean sslEnabled) {
        this.address = address;
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
            client = new Jedis(address.getHost(), address.getPort(), this.connectionTimeout, this.socketTimeout, this.sslEnabled);

            if (this.user != null) {
                client.auth(this.user, this.password);
            }
            else if (this.password != null) {
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
}
