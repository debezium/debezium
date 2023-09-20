/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.redis;

import java.time.Duration;
import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.util.DelayStrategy;

public class WaitReplicasRedisClient implements RedisClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(WaitReplicasRedisClient.class);

    private final RedisClient delegate;

    private final int replicas;

    private final long timeout;

    private final boolean retry;

    private final long delay;

    public WaitReplicasRedisClient(RedisClient delegate, int replicas, long timeout, boolean retry, long delay) {
        this.delegate = delegate;
        this.replicas = replicas;
        this.timeout = timeout;
        this.retry = retry;
        this.delay = delay;
    }

    @Override
    public void disconnect() {
        delegate.disconnect();
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public String xadd(String key, Map<String, String> hash) {
        return waitResult(() -> delegate.xadd(key, hash));
    }

    @Override
    public List<String> xadd(List<SimpleEntry<String, Map<String, String>>> hashes) {
        return waitResult(() -> delegate.xadd(hashes));
    }

    @Override
    public List<Map<String, String>> xrange(String key) {
        return delegate.xrange(key);
    }

    @Override
    public long xlen(String key) {
        return delegate.xlen(key);
    }

    @Override
    public Map<String, String> hgetAll(String key) {
        return delegate.hgetAll(key);
    }

    @Override
    public long hset(byte[] key, byte[] field, byte[] value) {
        return waitResult(() -> delegate.hset(key, field, value));
    }

    @Override
    public long waitReplicas(int replicas, long timeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "WaitReplicaRedisClient [delegate=" + delegate + ", replicas=" + replicas + ", timeout=" + timeout + ", retry=" + retry + ", delay=" + delay + "]";
    }

    private <R> R waitResult(Supplier<R> supplier) {
        R result;
        DelayStrategy delayStrategy = null;
        do {
            result = supplier.get();
            long reachedReplicas = delegate.waitReplicas(replicas, timeout);
            if (reachedReplicas != replicas) {
                if (retry) {
                    LOGGER.error("Failed to update {} replica(s) in {} millis. Retrying in {} millis...", replicas, timeout, delay);
                    if (delayStrategy == null) {
                        delayStrategy = DelayStrategy.constant(Duration.ofMillis(delay));
                    }
                    delayStrategy.sleepWhen(true);
                    continue;
                }
                else {
                    LOGGER.warn("Failed to update {} replica(s) in {} millis.", replicas, timeout);
                }
            }
            break;
        } while (true);
        return result;
    }

    @Override
    public String info(String section) {
        return delegate.info(section);
    }

    @Override
    public String clientList() {
        return delegate.clientList();
    }
}
