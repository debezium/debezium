/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

/**
 * Establishes a new connection to Redis
 *
 * @author Yossi Shirizli
 */
public class RedisConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisConnection.class);

    protected static final String DEBEZIUM_REDIS_SINK_CLIENT_NAME = "debezium:redis:sink";
    protected static final String DEBEZIUM_OFFSETS_CLIENT_NAME = "debezium:offsets";
    protected static final String DEBEZIUM_SCHEMA_HISTORY = "debezium:schema_history";

    private String address;
    private String user;
    private String password;
    private int connectionTimeout;
    private int socketTimeout;
    private boolean sslEnabled;

    public RedisConnection(String address, String user, String password, int connectionTimeout, int socketTimeout, boolean sslEnabled) {
        this.address = address;
        this.user = user;
        this.password = password;
        this.connectionTimeout = connectionTimeout;
        this.socketTimeout = socketTimeout;
        this.sslEnabled = sslEnabled;
    }

    public Jedis getRedisClient(String clientName) {
        HostAndPort address = HostAndPort.from(this.address);

        Jedis client = new Jedis(address.getHost(), address.getPort(), this.connectionTimeout, this.socketTimeout, this.sslEnabled);

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

        LOGGER.info("Using Jedis '{}'", client);

        return client;
    }
}
