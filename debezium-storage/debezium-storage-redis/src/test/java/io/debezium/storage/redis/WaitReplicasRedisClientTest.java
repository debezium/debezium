/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

public class WaitReplicasRedisClientTest {

    @Test
    public void xaddOneNoRetry() {
        String result = client(false).xadd(null, null);
        assertEquals("2", result);
    }

    @Test
    public void xaddOneRetry() {
        String result = client(true).xadd(null, null);
        assertEquals("0", result);
    }

    @Test
    public void xaddAllNoRetry() {
        List<String> result = client(false).xadd(null);
        assertEquals("2", result.get(0));
    }

    @Test
    public void xaddAllRetry() {
        List<String> result = client(true).xadd(null);
        assertEquals("0", result.get(0));
    }

    @Test
    public void xrangeNoRetry() {
        List<Map<String, String>> result = client(false).xrange("key");
        assertEquals("2", result.get(0).get("key"));
    }

    @Test
    public void xrangeRetry() {
        List<Map<String, String>> result = client(true).xrange("key");
        assertEquals("2", result.get(0).get("key"));
    }

    @Test
    public void xlenNoRetry() {
        long result = client(false).xlen("key");
        assertEquals(2, result);
    }

    @Test
    public void xlenRetry() {
        long result = client(true).xlen("key");
        assertEquals(2, result);
    }

    @Test
    public void hgetAllNoRetry() {
        Map<String, String> result = client(false).hgetAll("key");
        assertEquals("2", result.get("key"));
    }

    @Test
    public void hgetAllRetry() {
        Map<String, String> result = client(true).hgetAll("key");
        assertEquals("2", result.get("key"));
    }

    @Test
    public void hsetNoRetry() {
        long result = client(false).hset(null, null, null);
        assertEquals(2, result);
    }

    @Test
    public void hsetRetry() {
        long result = client(true).hset(null, null, null);
        assertEquals(0, result);
    }

    @Test
    public void waitUnsupported() {
        assertThrows(UnsupportedOperationException.class, () -> client(false).waitReplicas(0, 0));
    }

    private RedisClient client(boolean retry) {
        RedisClient client = new RedisClientImpl(2);
        RedisClient waitClient = new WaitReplicasRedisClient(client, 1, 1, retry, 1);
        return waitClient;
    }

    private static class RedisClientImpl implements RedisClient {

        private int errorCount;

        private RedisClientImpl(int errorCount) {
            this.errorCount = errorCount;
        }

        @Override
        public void disconnect() throws RedisClientConnectionException {
        }

        @Override
        public void close() throws RedisClientConnectionException {
        }

        @Override
        public String xadd(String key, Map<String, String> hash) throws RedisClientConnectionException {
            return errorCount();
        }

        @Override
        public List<String> xadd(List<SimpleEntry<String, Map<String, String>>> hashes) throws RedisClientConnectionException {
            List<String> result = new ArrayList<>();
            result.add(errorCount());
            return result;
        }

        @Override
        public List<Map<String, String>> xrange(String key) throws RedisClientConnectionException {
            List<Map<String, String>> result = new ArrayList<>();
            result.add(Collections.singletonMap(key, errorCount()));
            return result;
        }

        @Override
        public long xlen(String key) throws RedisClientConnectionException {
            return errorCount;
        }

        @Override
        public Map<String, String> hgetAll(String key) throws RedisClientConnectionException {
            return Collections.singletonMap(key, errorCount());
        }

        @Override
        public long hset(byte[] key, byte[] field, byte[] value) throws RedisClientConnectionException {
            return errorCount;
        }

        @Override
        public long waitReplicas(int replicas, long timeout) throws RedisClientConnectionException {
            return replicas + errorCount--;
        }

        private String errorCount() {
            return "" + errorCount;
        }

        @Override
        public String info(String section) {
            return "";
        }

        @Override
        public String clientList() {
            return null;
        }

    }

}
