/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.redis;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;

import redis.clients.jedis.ClusterPipeline;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Response;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.exceptions.JedisClusterOperationException;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.resps.StreamEntry;
import redis.clients.jedis.util.JedisClusterCRC16;

public class JedisClusterClient implements RedisClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(JedisClusterClient.class);

    private static final int MAX_ATTEMPTS = 10;
    private static final long DELAY_MILLIS = 200;

    private final JedisCluster jedisCluster;

    public JedisClusterClient(JedisCluster jedisCluster) {
        this.jedisCluster = jedisCluster;
        // Proactively initialize cluster slots cache with retries to handle transient startup/auth issues
        ensureClusterInitialized();
    }

    @Override
    public void disconnect() {
        tryErrors(jedisCluster::close);
    }

    @Override
    public void close() {
        tryErrors(jedisCluster::close);
    }

    @Override
    public String xadd(String key, Map<String, String> hash) {
        return tryErrors(() -> jedisCluster.xadd(key, (StreamEntryID) null, hash).toString());
    }

    @Override
    public List<String> xadd(List<SimpleEntry<String, Map<String, String>>> hashes) {
        return tryErrors(() -> {
            try {
                // Validate all keys belong to the same slot for pipelining
                validateKeysForPipelining(hashes);

                // Optionally ping to ensure the cluster is reachable before pipelining
                jedisCluster.ping();
                ClusterPipeline pipeline = jedisCluster.pipelined();
                List<Response<StreamEntryID>> responses = new ArrayList<>(hashes.size());
                hashes.forEach(hash -> responses.add(pipeline.xadd(hash.getKey(), StreamEntryID.NEW_ENTRY, hash.getValue())));
                pipeline.sync();
                return responses.stream().map(r -> r.get().toString()).toList();
            }
            catch (JedisDataException jde) {
                // When the Redis cluster is starting, a JedisDataException will be thrown with this message.
                // We will retry communicating with the target cluster as once Redis is available, this message will be gone.
                if (jde.getMessage().equals("LOADING Redis is loading the dataset in memory")) {
                    LOGGER.error("Redis cluster is starting", jde);
                }
                else {
                    LOGGER.error("Unexpected JedisDataException", jde);
                    throw new DebeziumException(jde);
                }
            }
            catch (JedisClusterOperationException crossSlotException) {
                LOGGER.warn("Cross-slot operation detected, falling back to sequential processing", crossSlotException);
                return fallbackSequentialXadd(hashes);
            }
            return Collections.emptyList();
        });
    }

    private void validateKeysForPipelining(List<SimpleEntry<String, Map<String, String>>> hashes) {
        if (hashes.isEmpty()) {
            return;
        }

        String firstKey = hashes.get(0).getKey();
        int expectedSlot = JedisClusterCRC16.getSlot(firstKey);

        for (SimpleEntry<String, Map<String, String>> hash : hashes) {
            int slot = JedisClusterCRC16.getSlot(hash.getKey());
            if (slot != expectedSlot) {
                throw new JedisClusterOperationException("Keys must belong to the same slot for pipelining");
            }
        }
    }

    private List<String> fallbackSequentialXadd(List<SimpleEntry<String, Map<String, String>>> hashes) {
        List<String> results = new ArrayList<>();
        for (SimpleEntry<String, Map<String, String>> hash : hashes) {
            try {
                String id = jedisCluster.xadd(hash.getKey(), StreamEntryID.NEW_ENTRY, hash.getValue()).toString();
                results.add(id);
            }
            catch (Exception e) {
                LOGGER.error("Failed to add entry for key: " + hash.getKey(), e);
                results.add(null);
            }
        }
        return results;
    }

    @Override
    public List<Map<String, String>> xrange(String key) {
        return tryErrors(() -> jedisCluster.xrange(key, (StreamEntryID) null, (StreamEntryID) null)
                .stream()
                .map(StreamEntry::getFields)
                .toList());
    }

    @Override
    public long xlen(String key) {
        return tryErrors(() -> jedisCluster.xlen(key));
    }

    @Override
    public Map<String, String> hgetAll(String key) {
        return tryErrors(() -> jedisCluster.hgetAll(key));
    }

    @Override
    public long hset(byte[] key, byte[] field, byte[] value) {
        return tryErrors(() -> jedisCluster.hset(key, field, value));
    }

    @Override
    public long waitReplicas(int replicas, long timeout) {
        // JedisCluster does not directly support waitReplicas across the entire cluster
        // Each shard manages its own replication independently
        throw new UnsupportedOperationException("waitReplicas is not directly supported in cluster mode. Each shard manages replication independently.");
    }

    @Override
    public String info(String section) {
        return tryErrors(() -> jedisCluster.info(section));
    }

    @Override
    public String clientList() {
        throw new UnsupportedOperationException("clientList is not directly supported in cluster mode.");
    }

    @Override
    public String toString() {
        return "JedisClusterClient [jedisCluster=" + jedisCluster + "]";
    }

    private void tryErrors(Runnable runnable) {
        tryErrors(() -> {
            runnable.run();
            return null;
        });
    }

    private void ensureClusterInitialized() {
        int tries = 0;
        while (tries < JedisClusterClient.MAX_ATTEMPTS) {
            try {
                jedisCluster.ping();
                return; // success
            }
            catch (JedisClusterOperationException | JedisConnectionException e) {
                tries++;
                if (tries >= JedisClusterClient.MAX_ATTEMPTS) {
                    throw new RedisClientConnectionException(e);
                }
                try {
                    Thread.sleep(JedisClusterClient.DELAY_MILLIS);
                }
                catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RedisClientConnectionException(e);
                }
            }
        }
    }

    private <R> R tryErrors(Supplier<R> supplier) {
        try {
            return supplier.get();
        }
        catch (JedisConnectionException | JedisClusterOperationException e) {
            throw new RedisClientConnectionException(e);
        }
    }
}
