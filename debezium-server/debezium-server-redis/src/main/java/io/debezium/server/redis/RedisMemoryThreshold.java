/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.storage.redis.RedisClient;
import io.debezium.util.IoUtil;
import io.smallrye.mutiny.tuples.Tuple2;

public class RedisMemoryThreshold {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisMemoryThreshold.class);

    private static final String INFO_MEMORY = "memory";
    private static final String INFO_MEMORY_SECTION_MAXMEMORY = "maxmemory";
    private static final String INFO_MEMORY_SECTION_USEDMEMORY = "used_memory";

    private static final Supplier<Boolean> MEMORY_OK = () -> true;

    private RedisClient client;

    private int memoryThreshold;

    private long memoryLimit;

    private Supplier<Boolean> isMemoryOk;

    public RedisMemoryThreshold(RedisClient client, RedisStreamChangeConsumerConfig config) {
        this.client = client;
        this.memoryThreshold = config.getMemoryThreshold();
        this.memoryLimit = 1024L * 1024 * config.getMemoryLimitMb();
        if (memoryThreshold == 0 || memoryTuple(memoryLimit) == null) {
            disable();
        }
        else {
            this.isMemoryOk = () -> isMemoryOk();
        }
    }

    public boolean check() {
        return isMemoryOk.get();
    }

    private boolean isMemoryOk() {
        Tuple2<Long, Long> memoryTuple = memoryTuple(memoryLimit);
        if (memoryTuple == null) {
            disable();
            return true;
        }
        long maxMemory = memoryTuple.getItem2();
        if (maxMemory > 0) {
            long usedMemory = memoryTuple.getItem1();
            long percentage = usedMemory * 100 / maxMemory;
            if (percentage >= memoryThreshold) {
                LOGGER.warn("Memory threshold percentage was reached (current: {}%, configured: {}%, used_memory: {}, maxmemory: {}).", percentage, memoryThreshold,
                        usedMemory, maxMemory);
                return false;
            }
        }
        return true;
    }

    private Tuple2<Long, Long> memoryTuple(long defaultMaxMemory) {
        String memory = client.info(INFO_MEMORY);
        Map<String, String> infoMemory = new HashMap<>();
        try {
            IoUtil.readLines(new ByteArrayInputStream(memory.getBytes(StandardCharsets.UTF_8)), line -> {
                String[] pair = line.split(":");
                if (pair.length == 2) {
                    infoMemory.put(pair[0], pair[1]);
                }
            });
        }
        catch (IOException e) {
            LOGGER.error("Cannot parse Redis 'info memory' result '{}'.", memory, e);
            return null;
        }

        Long usedMemory = parseLong(INFO_MEMORY_SECTION_USEDMEMORY, infoMemory.get(INFO_MEMORY_SECTION_USEDMEMORY));
        if (usedMemory == null) {
            return null;
        }

        Long maxMemory = parseLong(INFO_MEMORY_SECTION_MAXMEMORY, infoMemory.get(INFO_MEMORY_SECTION_MAXMEMORY));
        if (maxMemory == null) {
            if (defaultMaxMemory == 0) {
                LOGGER.warn("Memory limit is disabled '{}'.", defaultMaxMemory);
                return null;
            }
            LOGGER.debug("Using memory limit with value '{}'.", defaultMaxMemory);
            maxMemory = defaultMaxMemory;
        }
        else if (maxMemory == 0) {
            LOGGER.debug("Redis 'info memory' field '{}' is {}. Consider configuring it.", INFO_MEMORY_SECTION_MAXMEMORY, maxMemory);
            if (defaultMaxMemory > 0) {
                maxMemory = defaultMaxMemory;
                LOGGER.debug("Using memory limit with value '{}'.", defaultMaxMemory);
            }
        }

        return Tuple2.of(usedMemory, maxMemory);
    }

    private void disable() {
        isMemoryOk = MEMORY_OK;
        LOGGER.warn("Memory threshold percentage check is disabled!");
    }

    private Long parseLong(String name, String value) {
        try {
            return Long.valueOf(value);
        }
        catch (NumberFormatException e) {
            LOGGER.debug("Cannot parse Redis 'info memory' field '{}' with value '{}'.", name, value);
        }
        return null;
    }

}
