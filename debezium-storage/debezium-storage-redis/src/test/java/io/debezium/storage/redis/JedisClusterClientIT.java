/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.junit.jupiter.Container;

import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

/**
 * Integration tests for JedisClusterClient against a local Redis Cluster
 * composed of nodes at localhost:7001,7002,7003 with password "password".
 * <p>
 * Tests are skipped automatically if the cluster is not reachable.
 */
public class JedisClusterClientIT {

    public static final String HOSTNAME = "127.0.0.1";
    private static final String PASSWORD = "password";
    private static final int PORT_1 = 7001;
    private static final int PORT_2 = 7002;
    private static final int PORT_3 = 7003;
    private static final int[] PORTS = new int[]{PORT_1, PORT_2, PORT_3};
    private static final int CLUSTER_INIT_WAIT_MS = 30000;
    private static final int CONNECTION_TIMEOUT_MS = 300;
    private static final int CLUSTER_TIMEOUT_SECONDS = 5;

    @Container
    public static ComposeContainer redisCluster = new ComposeContainer(new File("src/test/resources/docker-compose.yml"))
            .withLocalCompose(true);

    private JedisCluster jedisCluster;
    private JedisClusterClient client;

    @BeforeAll
    static void init() throws InterruptedException {
        redisCluster.start();
        Thread.sleep(CLUSTER_INIT_WAIT_MS);
    }

    @BeforeEach
    void setUp() {
        assumeTrue(isClusterAvailable(), () -> "Skipping tests: Redis Cluster not available on localhost:7001-7003");
        Set<HostAndPort> nodes = Set.of(
                new HostAndPort(HOSTNAME, PORT_1),
                new HostAndPort(HOSTNAME, PORT_2),
                new HostAndPort(HOSTNAME, PORT_3));

        DefaultJedisClientConfig config = DefaultJedisClientConfig.builder()
                .password(PASSWORD)
                .timeoutMillis((int) Duration.ofSeconds(CLUSTER_TIMEOUT_SECONDS).toMillis())
                .build();

        jedisCluster = new JedisCluster(nodes, config);
        client = new JedisClusterClient(jedisCluster);
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            try {
                client.close();
            }
            catch (Exception ignored) {
                // Intentionally ignored
            }
        }
    }

    @AfterAll
    static void shutdown() {
        if (redisCluster != null) {
            redisCluster.stop();
        }
    }

    @Test
    @DisplayName("xadd(single) should append an entry and reflect in xlen/xrange")
    void testXAddSingle() {
        String key = "test:stream:" + UUID.randomUUID();
        Map<String, String> hash = new HashMap<>();
        hash.put("field1", "value1");
        hash.put("field2", "value2");

        String id = client.xadd(key, hash);
        assertNotNull(id);

        long len = client.xlen(key);
        assertEquals(1, len);

        List<Map<String, String>> entries = client.xrange(key);
        assertEquals(1, entries.size());
        assertEquals(hash, entries.get(0));
    }

    @Test
    @DisplayName("xadd(list) should pipeline multiple appends and return IDs")
    void testXAddListPipelined() {
        String key = "test:stream:" + UUID.randomUUID();

        // Prepare 3 entries for the same stream to avoid cross-slot issues
        List<SimpleEntry<String, Map<String, String>>> batch = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Map<String, String> h = new HashMap<>();
            h.put("i", Integer.toString(i));
            h.put("payload", UUID.randomUUID().toString());
            batch.add(new SimpleEntry<>(key, h));
        }

        List<String> ids = client.xadd(batch);
        assertNotNull(ids);
        assertEquals(3, ids.size());

        long len = client.xlen(key);
        assertEquals(3, len);
    }

    @Test
    @DisplayName("hset/hgetAll should set and retrieve hash fields")
    void testHashOperations() {
        String key = "test:hash:" + UUID.randomUUID();
        String field = "hello";
        String value = "world";

        long result = client.hset(key.getBytes(), field.getBytes(), value.getBytes());
        // the result is 1 if the field is a new field in the hash and value was set.
        assertTrue(result == 0 || result == 1);

        Map<String, String> all = client.hgetAll(key);
        assertNotNull(all);
        assertEquals(value, all.get(field));
    }

    @Test
    @DisplayName("info(server) should return non-empty info string")
    void testInfo() {
        String info = client.info("server");
        assertNotNull(info);
        assertFalse(info.isEmpty());
    }

    @Test
    @DisplayName("waitReplicas is unsupported in cluster client")
    void testWaitReplicasUnsupported() {
        assertThrows(UnsupportedOperationException.class, () -> client.waitReplicas(1, 1000));
    }

    @Test
    @DisplayName("clientList is unsupported in cluster client")
    void testClientListUnsupported() {
        assertThrows(UnsupportedOperationException.class, () -> client.clientList());
    }

    private boolean isClusterAvailable() {
        for (int port : PORTS) {
            if (canConnect(port)) {
                return true; // At least one node reachable
            }
        }
        return false;
    }

    private boolean canConnect(int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(HOSTNAME, port), JedisClusterClientIT.CONNECTION_TIMEOUT_MS);
            return true;
        }
        catch (IOException e) {
            return false;
        }
    }
}