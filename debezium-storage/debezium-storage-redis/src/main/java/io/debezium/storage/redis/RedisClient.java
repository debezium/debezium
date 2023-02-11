/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.redis;

import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;

public interface RedisClient {

    /**
     *
     * @throws RedisClientConnectionException
     */
    void disconnect();

    /**
     *
     * @throws RedisClientConnectionException
     */
    void close();

    /**
     *
     * @param key
     * @param hash
     * @return
     * @throws RedisClientConnectionException
     */
    String xadd(String key, Map<String, String> hash);

    /**
     *
     * @param hashes
     * @return
     * @throws RedisClientConnectionException
     */
    List<String> xadd(List<SimpleEntry<String, Map<String, String>>> hashes);

    /**
     *
     * @param key
     * @return
     * @throws RedisClientConnectionException
     */
    List<Map<String, String>> xrange(String key);

    /**
     *
     * @param key
     * @return
     * @throws RedisClientConnectionException
     */
    long xlen(String key);

    /**
     *
     * @param key
     * @return
     * @throws RedisClientConnectionException
     */
    Map<String, String> hgetAll(String key);

    /**
     *
     * @param key
     * @param field
     * @param value
     * @return
     * @throws RedisClientConnectionException
     */
    long hset(byte[] key, byte[] field, byte[] value);

    /**
     *
     * @param replicas
     * @param timeout
     * @return
     * @throws RedisClientConnectionException
     */
    long waitReplicas(int replicas, long timeout);

    /**
     *
     * @return
     * @throws RedisClientConnectionException
     */
    String info(String section);

}
