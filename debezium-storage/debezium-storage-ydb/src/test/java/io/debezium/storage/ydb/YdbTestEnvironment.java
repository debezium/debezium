/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.ydb;

import java.io.File;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ComposeContainer;

import tech.ydb.auth.NopAuthProvider;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.query.tools.SessionRetryContext;

/**
 * Boots a local YDB instance via docker-compose for integration tests. The compose file
 * exposes YDB on the host's {@code 2136} (grpc) and advertises {@code localhost} as the
 * discovery hostname so the SDK can reach it from outside the container.
 *
 * <p>Use {@link #start()} / {@link #stop()} from {@code @BeforeAll} / {@code @AfterAll}.
 */
public final class YdbTestEnvironment {

    private static final Logger LOGGER = LoggerFactory.getLogger(YdbTestEnvironment.class);

    public static final String HOST = "localhost";
    public static final int GRPC_PORT = 2136;
    public static final String ENDPOINT = "grpc://" + HOST + ":" + GRPC_PORT;
    public static final String DATABASE = "/local";

    private static final String COMPOSE_FILE = "src/test/resources/docker-compose-ydb.yml";
    private static final Duration READY_TIMEOUT = Duration.ofMinutes(3);
    private static final long POLL_INTERVAL_MS = 2_000;

    private static ComposeContainer compose;

    private YdbTestEnvironment() {
    }

    public static synchronized void start() throws InterruptedException {
        if (compose != null) {
            return;
        }
        compose = new ComposeContainer(new File(COMPOSE_FILE));
        compose.start();
        waitForYdbReady();
    }

    public static synchronized void stop() {
        if (compose != null) {
            compose.stop();
            compose = null;
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