/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.redis;

import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;

public class JedisClient implements RedisClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(JedisClient.class);

    private final Jedis jedis;

    public JedisClient(Jedis jedis) {
        this.jedis = jedis;
    }

    @Override
    public void disconnect() {
        tryErrors(() -> jedis.disconnect());
    }

    @Override
    public void close() {
        tryErrors(() -> jedis.close());
    }

    @Override
    public String xadd(String key, Map<String, String> hash) {
        return tryErrors(() -> jedis.xadd(key, (StreamEntryID) null, hash).toString());
    }

    @Override
    public List<String> xadd(List<SimpleEntry<String, Map<String, String>>> hashes) {
        return tryErrors(() -> {
            try {
                // Make sure the connection is still alive before creating the pipeline
                // to reduce the chance of ending up with duplicate records
                jedis.ping();
                Pipeline pipeline = jedis.pipelined();
                hashes.forEach((hash) -> pipeline.xadd(hash.getKey(), StreamEntryID.NEW_ENTRY, hash.getValue()));
                return pipeline.syncAndReturnAll().stream().map(response -> response.toString()).collect(Collectors.toList());
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
            return Collections.emptyList();
        });
    }

    @Override
    public List<Map<String, String>> xrange(String key) {
        return tryErrors(() -> jedis.xrange(key, (StreamEntryID) null, (StreamEntryID) null).stream().map(item -> item.getFields()).collect(Collectors.toList()));
    }

    @Override
    public long xlen(String key) {
        return tryErrors(() -> jedis.xlen(key));
    }

    @Override
    public Map<String, String> hgetAll(String key) {
        return tryErrors(() -> jedis.hgetAll(key));
    }

    @Override
    public long hset(byte[] key, byte[] field, byte[] value) {
        return tryErrors(() -> jedis.hset(key, field, value));
    }

    @Override
    public long waitReplicas(int replicas, long timeout) {
        return tryErrors(() -> jedis.waitReplicas(replicas, timeout));
    }

    @Override
    public String toString() {
        return "JedisClient [jedis=" + jedis + "]";
    }

    private void tryErrors(Runnable runnable) {
        tryErrors(() -> {
            runnable.run();
            return null;
        });
    }

    private <R> R tryErrors(Supplier<R> supplier) {
        try {
            return supplier.get();
        }
        catch (JedisConnectionException e) {
            throw new RedisClientConnectionException(e);
        }
    }

    @Override
    public String info(String section) {
        return tryErrors(() -> jedis.info(section));
    }

}
