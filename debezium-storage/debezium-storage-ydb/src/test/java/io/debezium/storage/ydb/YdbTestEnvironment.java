/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.ydb;

import java.time.Duration;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import tech.ydb.auth.NopAuthProvider;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.query.tools.SessionRetryContext;

/**
 * Boots a local YDB instance via Testcontainers for integration tests. The local-ydb image
 * advertises its discovery hostname to clients, so the container must be reachable at the same
 * hostname the SDK resolves — we fix it to {@code localhost} and bind the host port to the
 * advertised gRPC port (2136) via {@code createContainerCmdModifier}.
 *
 * <p>Use {@link #start()} / {@link #stop()} from {@code @BeforeAll} / {@code @AfterAll}.
 */
public final class YdbTestEnvironment {

    private static final Logger LOGGER = LoggerFactory.getLogger(YdbTestEnvironment.class);

    private static final DockerImageName IMAGE = DockerImageName.parse("ydbplatform/local-ydb:latest");
    private static final int GRPC_PORT = 2136;
    private static final int GRPC_TLS_PORT = 2135;
    private static final int MON_PORT = 8765;

    public static final String HOST = "localhost";
    public static final String ENDPOINT = "grpc://" + HOST + ":" + GRPC_PORT;
    public static final String DATABASE = "/local";

    private static final Duration READY_TIMEOUT = Duration.ofMinutes(3);
    private static final long POLL_INTERVAL_MS = 2_000;

    private static GenericContainer<?> container;

    private YdbTestEnvironment() {
    }

    @SuppressWarnings("resource")
    public static synchronized void start() throws InterruptedException {
        if (container != null) {
            return;
        }
        container = new GenericContainer<>(IMAGE)
                .withEnv("YDB_USE_IN_MEMORY_PDISKS", "true")
                .withEnv("YDB_DEFAULT_LOG_LEVEL", "NOTICE")
                .withEnv("GRPC_PORT", String.valueOf(GRPC_PORT))
                .withEnv("GRPC_TLS_PORT", String.valueOf(GRPC_TLS_PORT))
                .withEnv("MON_PORT", String.valueOf(MON_PORT))
                // The SDK connects to whatever hostname YDB discovery returns; the container
                // advertises its own hostname, so we force it to "localhost" and bind ports 1:1.
                .withCreateContainerCmdModifier(cmd -> cmd.withHostName(HOST))
                .withExposedPorts(GRPC_PORT, GRPC_TLS_PORT, MON_PORT)
                .waitingFor(Wait.forListeningPort().withStartupTimeout(READY_TIMEOUT));
        // Bind host port == container port: YDB discovery advertises the container's GRPC_PORT
        // and the SDK reconnects to that exact port on the host, so dynamic port mapping breaks.
        container.setPortBindings(List.of(
                GRPC_PORT + ":" + GRPC_PORT,
                GRPC_TLS_PORT + ":" + GRPC_TLS_PORT,
                MON_PORT + ":" + MON_PORT));
        container.start();
        waitForYdbReady();
    }

    public static synchronized void stop() {
        if (container != null) {
            container.stop();
            container = null;
        }
    }

    /**
     * Actively probes YDB by creating-and-dropping a throwaway table. Listing the database root
     * succeeds before storage pools are bound to {@code /local}; only a real DDL against the
     * database confirms that subsequent {@code CREATE TABLE IF NOT EXISTS} calls from the
     * storage code will not fail with {@code "database doesn't have storage pools"}.
     */
    private static void waitForYdbReady() throws InterruptedException {
        long deadline = System.currentTimeMillis() + READY_TIMEOUT.toMillis();
        Throwable lastError = null;
        int attempt = 0;
        String probeTable = "dbz_ydb_readiness_probe";
        String createDdl = "CREATE TABLE IF NOT EXISTS `" + probeTable + "` (id Utf8, PRIMARY KEY(id))";
        String dropDdl = "DROP TABLE IF EXISTS `" + probeTable + "`";
        while (System.currentTimeMillis() < deadline) {
            attempt++;
            try (GrpcTransport transport = GrpcTransport
                    .forConnectionString(ENDPOINT + "?database=" + DATABASE)
                    .withAuthProvider(NopAuthProvider.INSTANCE)
                    .build();
                    QueryClient queryClient = QueryClient.newClient(transport).build()) {
                SessionRetryContext retryCtx = SessionRetryContext.create(queryClient).build();
                retryCtx.supplyResult(s -> s.createQuery(createDdl, TxMode.NONE).execute())
                        .join().getStatus().expectSuccess("readiness CREATE");
                retryCtx.supplyResult(s -> s.createQuery(dropDdl, TxMode.NONE).execute())
                        .join().getStatus().expectSuccess("readiness DROP");
                LOGGER.info("YDB ready after {} probe(s)", attempt);
                return;
            }
            catch (Throwable t) {
                lastError = t;
                Thread.sleep(POLL_INTERVAL_MS);
            }
        }
        throw new IllegalStateException("YDB did not become ready on " + ENDPOINT
                + " within " + READY_TIMEOUT + " (last error: " + lastError + ")", lastError);
    }
}
