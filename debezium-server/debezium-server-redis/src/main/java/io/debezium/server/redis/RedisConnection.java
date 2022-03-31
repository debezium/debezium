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

/**
 * Establishes a new connection to Redis
 *
 * @author Yossi Shirizli
 */
public class RedisConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisConnection.class);

    protected static final String DEBEZIUM_REDIS_SINK_CLIENT_NAME = "debezium:redis:sink";
    protected static final String DEBEZIUM_OFFSETS_CLIENT_NAME = "debezium:offsets";
    protected static final String DEBEZIUM_DB_HISTORY = "debezium:db_history";

    private String address;
    private String user;
    private String password;
    private boolean sslEnabled;

    public RedisConnection(String address, String user, String password, boolean sslEnabled) {
        this.address = address;
        this.user = user;
        this.password = password;
        this.sslEnabled = sslEnabled;
    }

    public Jedis getRedisClient(String clientName) {
        HostAndPort address = HostAndPort.from(this.address);

        Jedis client = new Jedis(address.getHost(), address.getPort(), this.sslEnabled);
        client.clientSetname(clientName);

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

        LOGGER.info("Using Jedis '{}'", client);

        return client;
    }
}
