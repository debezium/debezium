/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import java.util.Map;

public class RedisStreamMemoryThresholdTestProfile extends RedisStreamTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
        Map<String, String> config = super.getConfigOverrides();
        config.put("debezium.sink.redis.memory.threshold.percentage", "75");
        return config;
    }

}
