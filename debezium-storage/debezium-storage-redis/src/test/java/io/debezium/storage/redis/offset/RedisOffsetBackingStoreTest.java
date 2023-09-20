/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.redis.offset;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import io.debezium.config.Configuration;
import io.debezium.storage.redis.RedisClient;
import io.debezium.storage.redis.RedisClientConnectionException;
import io.debezium.storage.redis.RedisConnection;

@Testcontainers
class RedisOffsetBackingStoreTest {
    @Container
    public GenericContainer redis = new GenericContainer(DockerImageName.parse(REDIS_CONTAINER_IMAGE))
            .withExposedPorts(6379);
    private static final String PROP_PREFIX = "offset.storage.redis.";
    private static final String REDIS_CONTAINER_IMAGE = "redis:5.0.3-alpine";
    private static final String NEW_LINE = "\n";
    private String address;
    private int port;

    @BeforeEach
    public void setUp() {
        redis.start();
        this.address = redis.getHost();
        this.port = redis.getFirstMappedPort();
    }

    @AfterEach
    public void tearDown() {
        if (redis != null) {
            redis.stop();
        }
    }

    @Test
    public void testRedisConnection() throws InterruptedException {
        RedisOffsetBackingStoreConfig config = getRedisOffsetBackingStoreConfig();
        RedisOffsetBackingStore redisOffsetBackingStore = new RedisOffsetBackingStore();
        redisOffsetBackingStore.configure(config);

        redisOffsetBackingStore.startNoLoad();

        RedisClient client = redisOffsetBackingStore.getRedisClient();
        int clientsNum = (int) getClientsNumber(RedisConnection.DEBEZIUM_OFFSETS_CLIENT_NAME, client);
        assert (clientsNum == 1);

        RedisClient mockClient = Mockito.spy(client);
        when(mockClient.hgetAll(anyString())).thenThrow(RedisClientConnectionException.class);
        redisOffsetBackingStore.setRedisClient(mockClient);
        redisOffsetBackingStore.load();
        client = redisOffsetBackingStore.getRedisClient();
        clientsNum = (int) getClientsNumber(RedisConnection.DEBEZIUM_OFFSETS_CLIENT_NAME, client);
        assert (clientsNum == 1);

        redisOffsetBackingStore.stop();
        assert (redisOffsetBackingStore.getRedisClient() == null);
    }

    private long getClientsNumber(String name, RedisClient client) {
        return Arrays.stream(client.clientList().split(NEW_LINE)).filter(entry -> entry.contains(name)).count();
    }

    private RedisOffsetBackingStoreConfig getRedisOffsetBackingStoreConfig() {
        Map<String, String> dummyConfig = new HashMap<>();
        dummyConfig.put(PROP_PREFIX + "address", this.address + ":" + this.port);
        return new RedisOffsetBackingStoreConfig(Configuration.from(dummyConfig));
    }
}
