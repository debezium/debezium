/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import io.debezium.config.Configuration;
import io.debezium.storage.redis.offset.RedisOffsetBackingStoreConfig;

/**
 * Verifies that {@code WAIT} actually covers the write it is paired with, for every supported client
 * library. Each test runs against both Jedis and Lettuce with identical assertions, so the suite doubles as
 * the behavioural parity check between the two drivers.
 * <p>
 * Redis evaluates {@code WAIT} against {@code c->woff}, the replication offset recorded for the
 * <em>calling</em> connection after its own most recent command. An implementation that sends the write
 * over one connection and the {@code WAIT} over another therefore has the {@code WAIT} judge the offset as
 * it stood <em>before</em> that write: it reports success for the preceding state and never covers the
 * write it was paired with, silently weakening the guarantee {@code wait.enabled=true} is meant to buy.
 * <p>
 * Detecting that lag needs the preceding offset to be acknowledged while the write under test is not, so
 * the tests write once, wait for the replica to catch up, freeze the replica so that it can no longer send
 * {@code REPLCONF ACK}, and only then perform the write they measure. A correct implementation blocks for
 * the whole wait timeout; one that splits the two commands across connections returns immediately.
 */
@Testcontainers
class LettuceWaitReplicasIT {

    private static final String REDIS_CONTAINER_IMAGE = "redis:5.0.3-alpine";
    private static final String PROP_PREFIX = "offset.storage.redis.";
    private static final String MASTER_ALIAS = "redis-master";

    /**
     * Must stay below the socket timeout: that value is the Jedis socket read timeout and the Lettuce
     * command timeout alike, and a {@code WAIT} that blocks longer than it trips the driver before the
     * server replies. The defaults (1000 ms wait, 2000 ms socket) leave the same headroom.
     */
    private static final long WAIT_TIMEOUT_MS = 1000L;
    private static final long SOCKET_TIMEOUT_MS = 3000L;

    /**
     * WAIT blocks for the whole timeout, so a correct implementation cannot come back much earlier than
     * this. A broken one returns in single-digit milliseconds, so the exact bound is not delicate.
     */
    private static final long BLOCKED_THRESHOLD_MS = WAIT_TIMEOUT_MS * 3 / 4;

    private Network network;
    private GenericContainer<?> master;
    private GenericContainer<?> replica;
    private RedisClient client;

    @BeforeEach
    public void setUp() {
        network = Network.newNetwork();

        master = new GenericContainer<>(DockerImageName.parse(REDIS_CONTAINER_IMAGE))
                .withNetwork(network)
                .withNetworkAliases(MASTER_ALIAS)
                .withExposedPorts(6379);
        master.start();

        replica = new GenericContainer<>(DockerImageName.parse(REDIS_CONTAINER_IMAGE))
                .withNetwork(network)
                .withCommand("redis-server", "--replicaof", MASTER_ALIAS, "6379")
                .withExposedPorts(6379);
        replica.start();

        awaitReplicaOnline();
    }

    @AfterEach
    public void tearDown() {
        if (client != null) {
            client.close();
            client = null;
        }
        unpauseReplica();
        if (replica != null) {
            replica.stop();
            replica = null;
        }
        if (master != null) {
            master.stop();
            master = null;
        }
        if (network != null) {
            network.close();
            network = null;
        }
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(RedisClientLibrary.class)
    public void hsetShouldBlockWhileTheReplicaCannotAcknowledge(RedisClientLibrary library) {
        client = newClient(library, WAIT_TIMEOUT_MS);

        // A healthy replica acknowledges quickly, so the same call is fast before the replica is frozen.
        long healthy = timeOf(() -> client.hset("dbz:offsets".getBytes(), "warmup".getBytes(), "v".getBytes()));
        assertTrue(healthy < BLOCKED_THRESHOLD_MS,
                "with a healthy replica hset should not block for the wait timeout, but took " + healthy + " ms");

        // The replica must have acknowledged everything written so far, otherwise a WAIT that lags by one
        // write still blocks on the backlog and the split-connection defect stays invisible.
        awaitReplicaCaughtUp();
        pauseReplica();

        long frozen = timeOf(() -> client.hset("dbz:offsets".getBytes(), "field".getBytes(), "value".getBytes()));
        assertTrue(frozen >= BLOCKED_THRESHOLD_MS,
                "hset returned after " + frozen + " ms; WAIT did not cover the write, which means it was issued "
                        + "on a different connection than the HSET");
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(RedisClientLibrary.class)
    public void xaddShouldBlockWhileTheReplicaCannotAcknowledge(RedisClientLibrary library) {
        client = newClient(library, WAIT_TIMEOUT_MS);

        pauseReplica();

        long frozen = timeOf(() -> client.xadd("dbz:stream", Map.of("field", "value")));
        assertTrue(frozen >= BLOCKED_THRESHOLD_MS,
                "xadd returned after " + frozen + " ms; WAIT did not cover the write");
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(RedisClientLibrary.class)
    public void pipelinedXaddShouldBlockWhileTheReplicaCannotAcknowledge(RedisClientLibrary library) {
        // The batched overload is the one the stream change consumer uses, and Lettuce sends it through the
        // async view of the connection while WAIT goes through the sync view. Both views share the single
        // connection, which is exactly what this asserts.
        client = newClient(library, WAIT_TIMEOUT_MS);

        List<SimpleEntry<String, Map<String, String>>> batch = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            batch.add(new SimpleEntry<>("dbz:stream:batch", Map.of("i", String.valueOf(i))));
        }

        pauseReplica();

        long frozen = timeOf(() -> client.xadd(batch));
        assertTrue(frozen >= BLOCKED_THRESHOLD_MS,
                "pipelined xadd returned after " + frozen + " ms; WAIT did not cover the batch");
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(RedisClientLibrary.class)
    public void shouldOpenASingleConnection(RedisClientLibrary library) {
        client = newClient(library, WAIT_TIMEOUT_MS);

        // A second connection is what breaks WAIT above; CLIENT LIST makes the regression visible directly.
        // Counting by client name would miss it, because an extra connection need not carry the name, so
        // every client connection is counted and only the replication link (flags=S) is excluded.
        long connections = client.clientList().lines()
                .filter(line -> !line.isBlank())
                .filter(line -> !line.contains("flags=S"))
                .count();
        assertEquals(1L, connections,
                "expected a single connection, otherwise writes and WAIT can end up on different connections, but CLIENT LIST was:\n"
                        + client.clientList());
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(RedisClientLibrary.class)
    public void waitTimeoutAboveTheSocketTimeoutShouldSurfaceAsAConnectionFailure(RedisClientLibrary library) {
        // Both drivers derive their read/command timeout from redis.socket.timeout, so a WAIT that is allowed
        // to block longer than that trips the driver before the server answers. Pinned here so the shared
        // constraint stays visible, and so the two libraries are known to behave the same way.
        client = newClient(library, SOCKET_TIMEOUT_MS * 2);

        pauseReplica();

        assertThrows(RedisClientConnectionException.class,
                () -> client.hset("dbz:offsets".getBytes(), "field".getBytes(), "value".getBytes()));
    }

    private RedisClient newClient(RedisClientLibrary library, long waitTimeoutMs) {
        Map<String, String> props = new HashMap<>();
        props.put(PROP_PREFIX + "address", master.getHost() + ":" + master.getFirstMappedPort());
        props.put(PROP_PREFIX + "client.library", library.getValue());
        props.put(PROP_PREFIX + "socket.timeout.ms", String.valueOf(SOCKET_TIMEOUT_MS));
        RedisOffsetBackingStoreConfig config = new RedisOffsetBackingStoreConfig(Configuration.from(props));

        return RedisConnection.getInstance(config)
                .getRedisClient(RedisConnection.DEBEZIUM_OFFSETS_CLIENT_NAME, true, waitTimeoutMs, false, 0);
    }

    private long timeOf(Runnable action) {
        long start = System.nanoTime();
        action.run();
        return Duration.ofNanos(System.nanoTime() - start).toMillis();
    }

    private void awaitReplicaOnline() {
        long deadline = System.currentTimeMillis() + 30_000L;
        while (System.currentTimeMillis() < deadline) {
            if (execInMaster("redis-cli", "info", "replication").contains("state=online")) {
                return;
            }
            sleep(250);
        }
        throw new IllegalStateException("The replica did not come online: " + execInMaster("redis-cli", "info", "replication"));
    }

    private void awaitReplicaCaughtUp() {
        long deadline = System.currentTimeMillis() + 30_000L;
        while (System.currentTimeMillis() < deadline) {
            String info = execInMaster("redis-cli", "info", "replication");
            long masterOffset = parseLong(info, "master_repl_offset:");
            long replicaOffset = parseLong(info, "offset=");
            if (masterOffset >= 0 && masterOffset == replicaOffset) {
                return;
            }
            sleep(100);
        }
        throw new IllegalStateException("The replica never caught up: " + execInMaster("redis-cli", "info", "replication"));
    }

    private static long parseLong(String info, String token) {
        int start = info.indexOf(token);
        if (start < 0) {
            return -1;
        }
        start += token.length();
        int end = start;
        while (end < info.length() && Character.isDigit(info.charAt(end))) {
            end++;
        }
        return end == start ? -1 : Long.parseLong(info.substring(start, end));
    }

    private void pauseReplica() {
        DockerClientFactory.instance().client().pauseContainerCmd(replica.getContainerId()).exec();
        // The master keeps counting the replica as connected; it simply stops receiving REPLCONF ACK.
    }

    private void unpauseReplica() {
        if (replica == null || !replica.isRunning()) {
            return;
        }
        try {
            DockerClientFactory.instance().client().unpauseContainerCmd(replica.getContainerId()).exec();
        }
        catch (RuntimeException e) {
            // Already running; nothing to undo.
        }
    }

    private String execInMaster(String... command) {
        try {
            return master.execInContainer(command).getStdout();
        }
        catch (Exception e) {
            throw new IllegalStateException("Failed to run " + String.join(" ", command) + " in the master container", e);
        }
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
