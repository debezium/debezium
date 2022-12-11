/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.storage.redis.RedisClient;
import io.debezium.util.Collect;

public class RedisMemoryThresholdTest {

    private static final String _1MB = String.valueOf(1 * 1024 * 1024);
    private static final String _2MB = String.valueOf(2 * 1024 * 1024);
    private static final String _3MB = String.valueOf(3 * 1024 * 1024);
    private static final String _4MB = String.valueOf(4 * 1024 * 1024);

    @Test
    public void testThresholdPercentageDisabled() {
        int[] thresholdList = { 0 };
        int[] limitMbList = { 0, 1, 2, 3, 4 };
        String[] usedMemoryList = { "asd3f", "2048L", null, _1MB, _2MB, _3MB, _4MB };
        String[] maxMemoryList = { "asd3f", "2048L", null, "0", _1MB, _2MB, _3MB, _4MB };

        for (int threshold : thresholdList) {
            for (int limit : limitMbList) {
                for (String used : usedMemoryList) {
                    for (String max : maxMemoryList) {
                        isMemoryOk(threshold, limit, used, max, true);
                    }
                }
            }
        }
    }

    @Test
    public void testUsedMemoryBad() {
        int[] thresholdList = { 1, 24, 25, 26, 49, 50, 52, 74, 75, 76, 99, 100 };
        int[] limitMbList = { 0, 1, 2, 3, 4 };
        String[] usedMemoryList = { "asd3f", "2048L" };
        String[] maxMemoryList = { "asd3f", "2048L", null, "0", _1MB, _2MB, _3MB, _4MB };

        for (int threshold : thresholdList) {
            for (int limit : limitMbList) {
                for (String used : usedMemoryList) {
                    for (String max : maxMemoryList) {
                        isMemoryOk(threshold, limit, used, max, true);
                    }
                }
            }
        }
    }

    @Test
    public void testUsedMemoryNotReported() {
        int[] thresholdList = { 1, 24, 25, 26, 49, 50, 52, 74, 75, 76, 99, 100 };
        int[] limitMbList = { 0, 1, 2, 3, 4 };
        String[] usedMemoryList = { null };
        String[] maxMemoryList = { "asd3f", "2048L", null, "0", _1MB, _2MB, _3MB, _4MB };

        for (int threshold : thresholdList) {
            for (int limit : limitMbList) {
                for (String used : usedMemoryList) {
                    for (String max : maxMemoryList) {
                        isMemoryOk(threshold, limit, used, max, true);
                    }
                }
            }
        }
    }

    @Test
    public void testMemoryLimit() {
        int[] thresholdList = { 1, 24, 25, 26, 49, 50, 52, 74, 75, 76, 99, 100 };
        int[] limitMbList = { 0, 1, 2, 3, 4 };
        String[] usedMemoryList = { _1MB, _2MB, _3MB, _4MB };
        String[] maxMemoryList = { "asd3f", "2048L", null, "0" };

        for (int threshold : thresholdList) {
            for (int limit : limitMbList) {
                for (String used : usedMemoryList) {
                    for (String max : maxMemoryList) {
                        isMemoryOk(threshold, limit, used, max, 0 == limit ? true : Long.parseLong(used) * 100 / (limit * 1024 * 1024) < threshold);
                    }
                }
            }
        }
    }

    @Test
    public void testMaxMemory() {
        int[] thresholdList = { 1, 24, 25, 26, 49, 50, 52, 74, 75, 76, 99, 100 };
        int[] limitMbList = { 0, 1, 2, 3, 4 };
        String[] usedMemoryList = { _1MB, _2MB, _3MB, _4MB };
        String[] maxMemoryList = { _1MB, _2MB, _3MB, _4MB };

        for (int threshold : thresholdList) {
            for (int limit : limitMbList) {
                for (String used : usedMemoryList) {
                    for (String max : maxMemoryList) {
                        isMemoryOk(threshold, limit, used, max, Long.parseLong(used) * 100 / Long.parseLong(max) < threshold);
                    }
                }
            }
        }
    }

    private void isMemoryOk(int threshold, int memoryLimitMb, String usedMemoryBytes, String maxMemoryBytes, boolean expectedResult) {
        Configuration config = Configuration.from(Collect.hashMapOf("debezium.sink.redis.address", "localhost", "debezium.sink.redis.memory.threshold.percentage",
                threshold, "debezium.sink.redis.memory.limit.mb", memoryLimitMb));
        RedisMemoryThreshold isMemoryOk = new RedisMemoryThreshold(new RedisClientImpl(usedMemoryBytes, maxMemoryBytes), new RedisStreamChangeConsumerConfig(config));
        Assert.assertEquals(String.format("isMemoryOk failed for threshold %s, limit %s, used %s, max %s)", threshold, memoryLimitMb, usedMemoryBytes, maxMemoryBytes),
                expectedResult, isMemoryOk.check());
    }

    private static class RedisClientImpl implements RedisClient {

        private String infoMemory;

        private RedisClientImpl(String usedMemoryBytes, String maxMemoryBytes) {
            this.infoMemory = (usedMemoryBytes == null ? "" : "used_memory:" + usedMemoryBytes + "\n") + (maxMemoryBytes == null ? "" : "maxmemory:" + maxMemoryBytes);
        }

        @Override
        public String info(String section) {
            return infoMemory;
        }

        @Override
        public void disconnect() {
        }

        @Override
        public void close() {
        }

        @Override
        public String xadd(String key, Map<String, String> hash) {
            return null;
        }

        @Override
        public List<String> xadd(List<SimpleEntry<String, Map<String, String>>> hashes) {
            return null;
        }

        @Override
        public List<Map<String, String>> xrange(String key) {
            return null;
        }

        @Override
        public long xlen(String key) {
            return 0;
        }

        @Override
        public Map<String, String> hgetAll(String key) {
            return null;
        }

        @Override
        public long hset(byte[] key, byte[] field, byte[] value) {
            return 0;
        }

        @Override
        public long waitReplicas(int replicas, long timeout) {
            return 0;
        }

    }

}
