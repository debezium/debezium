/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.redis.offset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import io.debezium.config.Configuration;
import io.debezium.storage.redis.RedisClient;
import io.debezium.storage.redis.RedisClientConnectionException;
import io.debezium.storage.redis.RedisConnection;
import io.debezium.util.Testing;

@Testcontainers
class RedisOffsetBackingStoreIT {
    // Single Redis instance container
    @Container
    public GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse(REDIS_CONTAINER_IMAGE))
            .withExposedPorts(6379);

    // Redis cluster container - only used when cluster tests are enabled
    public static ComposeContainer redisCluster;

    private static final String PROP_PREFIX = "offset.storage.redis.";
    private static final String REDIS_CONTAINER_IMAGE = "redis:5.0.3-alpine";
    private static final String NEW_LINE = "\n";

    // Cluster configuration constants
    public static final String HOSTNAME = "127.0.0.1";
    private static final String PASSWORD = "password";
    private static final int PORT_1 = 7001;
    private static final int PORT_2 = 7002;
    private static final int PORT_3 = 7003;
    private static final int[] PORTS = new int[]{ PORT_1, PORT_2, PORT_3 };
    private static final int CLUSTER_INIT_WAIT_MS = 30000;
    private static final int CONNECTION_TIMEOUT_MS = 300;

    private String address;
    private int port;

    @BeforeAll
    static void initCluster() {
        try {
            redisCluster = new ComposeContainer(new File("src/test/resources/docker-compose-redis-cluster.yml"))
                    .withLocalCompose(true);
            redisCluster.start();
            Thread.sleep(CLUSTER_INIT_WAIT_MS);
        }
        catch (Exception e) {
            // If cluster setup fails, tests will be skipped via assumeTrue
            Testing.printError("Redis cluster setup failed", e);
            redisCluster = null;
        }
    }

    @BeforeEach
    public void setUp() {
        redis.start();
        this.address = redis.getHost();
        this.port = redis.getFirstMappedPort();
    }

    /**
     * Provides test parameters for cluster enabled/disabled scenarios.
     *
     * @return Stream of Arguments containing cluster enabled flag
     */
    static Stream<Arguments> clusterScenarios() {
        return Stream.of(
                Arguments.of(false),
                Arguments.of(true));
    }

    @AfterEach
    public void tearDown() {
        if (redis != null) {
            redis.stop();
        }
    }

    @AfterAll
    static void shutdownCluster() {
        if (redisCluster != null) {
            redisCluster.stop();
        }
    }

    @ParameterizedTest
    @MethodSource("clusterScenarios")
    @DisplayName("Test Redis connection with cluster mode")
    public void testRedisConnection(boolean clusterEnabled) throws InterruptedException {
        RedisOffsetBackingStore redisOffsetBackingStore = getRedisOffsetBackingStore(clusterEnabled);
        RedisClient client = redisOffsetBackingStore.getRedisClient();

        // For cluster mode, clientList is not supported, so we skip this assertion
        if (!clusterEnabled) {
            int clientsNum = (int) getClientsNumber(RedisConnection.DEBEZIUM_OFFSETS_CLIENT_NAME, client);
            assertEquals(clientsNum, 1);
        }

        redisOffsetBackingStore.stop();
        assertNull(redisOffsetBackingStore.getRedisClient());
    }

    @ParameterizedTest
    @MethodSource("clusterScenarios")
    @Timeout(5) // Ensure we don't stuck in endless retry loop
    @DisplayName("Test load with retry mechanism")
    public void testLoadWithRetry(boolean clusterEnabled) throws InterruptedException {
        RedisOffsetBackingStore redisOffsetBackingStore = getRedisOffsetBackingStore(clusterEnabled);
        RedisClient client = redisOffsetBackingStore.getRedisClient();

        RedisClient mockClient = Mockito.spy(client);
        when(mockClient.hgetAll(anyString())).thenThrow(RedisClientConnectionException.class);
        redisOffsetBackingStore.setRedisClient(mockClient);
        redisOffsetBackingStore.load();
        client = redisOffsetBackingStore.getRedisClient();

        // For cluster mode, clientList is not supported, so we skip this assertion
        if (!clusterEnabled) {
            int clientsNum = (int) getClientsNumber(RedisConnection.DEBEZIUM_OFFSETS_CLIENT_NAME, client);
            assertEquals(clientsNum, 1);
        }

        redisOffsetBackingStore.stop();
    }

    @ParameterizedTest
    @MethodSource("clusterScenarios")
    @Timeout(5) // Ensure we don't stuck in endless retry loop
    @DisplayName("Test save with retry mechanism")
    public void testSaveWithRetry(boolean clusterEnabled) throws InterruptedException {
        RedisOffsetBackingStore redisOffsetBackingStore = getRedisOffsetBackingStore(clusterEnabled);
        RedisClient client = redisOffsetBackingStore.getRedisClient();

        // Load test key-value pair to redis to able to test save()
        // besides redisKey, values of field and value are not important for this test
        byte[] redisKey = "metadata:debezium:offsets".getBytes();
        byte[] field = "".getBytes();
        byte[] value = "".getBytes();
        client.hset(redisKey, field, value);
        redisOffsetBackingStore.load();

        RedisClient mockClient = Mockito.spy(client);
        when(mockClient.hset(redisKey, field, value)).thenThrow(RedisClientConnectionException.class);
        redisOffsetBackingStore.setRedisClient(mockClient);
        redisOffsetBackingStore.save();
        client = redisOffsetBackingStore.getRedisClient();

        // For cluster mode, clientList is not supported, so we skip this assertion
        if (!clusterEnabled) {
            int clientsNum = (int) getClientsNumber(RedisConnection.DEBEZIUM_OFFSETS_CLIENT_NAME, client);
            assertEquals(clientsNum, 1);
        }

        redisOffsetBackingStore.stop();
    }

    private long getClientsNumber(String name, RedisClient client) {
        return Arrays.stream(client.clientList().split(NEW_LINE)).filter(entry -> entry.contains(name)).count();
    }

    private RedisOffsetBackingStore getRedisOffsetBackingStore(boolean clusterEnabled) {
        RedisOffsetBackingStoreConfig config = getRedisOffsetBackingStoreConfig(clusterEnabled);
        RedisOffsetBackingStore redisOffsetBackingStore = new RedisOffsetBackingStore();
        redisOffsetBackingStore.configure(config);
        redisOffsetBackingStore.startNoLoad();
        return redisOffsetBackingStore;
    }

    private RedisOffsetBackingStoreConfig getRedisOffsetBackingStoreConfig(boolean clusterEnabled) {
        Map<String, String> dummyConfig = new HashMap<>();

        if (clusterEnabled) {
            // For cluster mode, use cluster addresses
            assumeTrue(isClusterAvailable(), () -> "Skipping cluster tests: Redis cluster not available on localhost:7001-7003");
            String clusterAddresses = HOSTNAME + ":" + PORT_1 + "," + HOSTNAME + ":" + PORT_2 + "," + HOSTNAME + ":" + PORT_3;
            dummyConfig.put(PROP_PREFIX + "address", clusterAddresses);
            dummyConfig.put(PROP_PREFIX + "password", PASSWORD);
        }
        else {
            // For single instance mode, use single Redis address
            dummyConfig.put(PROP_PREFIX + "address", this.address + ":" + this.port);
        }

        dummyConfig.put(PROP_PREFIX + "cluster.enabled", String.valueOf(clusterEnabled));
        return new RedisOffsetBackingStoreConfig(Configuration.from(dummyConfig));
    }

    private boolean isClusterAvailable() {
        if (redisCluster == null) {
            return false;
        }
        for (int port : PORTS) {
            if (canConnect(port)) {
                return true; // At least one node reachable
            }
        }
        return false;
    }

    private boolean canConnect(int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(HOSTNAME, port), CONNECTION_TIMEOUT_MS);
            return true;
        }
        catch (IOException e) {
            return false;
        }
    }
}
