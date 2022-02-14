package io.debezium.server.redis;

import java.util.HashMap;
import java.util.Map;

import io.quarkus.test.junit.QuarkusTestProfile;

public class RedisOffsetTestProfile implements QuarkusTestProfile {

    public Map<String, String> getConfigOverrides() {
        Map<String, String> config = new HashMap<String, String>();
        config.put("debezium.source.offset.storage", "io.debezium.server.redis.RedisOffsetBackingStore");
        return config;
    }

}
