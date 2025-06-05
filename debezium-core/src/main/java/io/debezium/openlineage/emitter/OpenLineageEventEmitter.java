/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage.emitter;

import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.Module;
import io.debezium.openlineage.DebeziumOpenLineageConfiguration;
import io.openlineage.client.Clients;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;

public class OpenLineageEventEmitter {

    private static final Logger LOGGER = LoggerFactory.getLogger(OpenLineageEventEmitter.class);

    private static final String OPEN_LINEAGE_PRODUCER_URI_FORMAT = "https://github.com/debezium/debezium/%s";
    private static final String NOT_AVAILABLE = "N/A";
    private static final String SNAPSHOT = "SNAPSHOT";
    private static final String MAIN_BRANCH_NAME = "main";
    private static final String VERSION_FORMAT = "v%s";

    private final ExecutorService emitterExecutor = Executors.newSingleThreadExecutor();
    private final OpenLineageClient openLineageClient;

    public OpenLineageEventEmitter(DebeziumOpenLineageConfiguration config) {

        if (config.enabled()) {
            openLineageClient = Clients.newClient(() -> List.of(
                    Path.of(config.config().path())));
        }
        else {
            openLineageClient = null;
        }

        LOGGER.debug("OpenLineage client v{} configured", getClientVersion());
    }

    public OpenLineageEventEmitter(OpenLineageClient client) {
        this.openLineageClient = client;
    }

    public void emit(OpenLineage.RunEvent event) {

        CompletableFuture.supplyAsync(() -> {
            try {
                openLineageClient.emit(event);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
            return null;
        }, emitterExecutor)
                .exceptionally(throwable -> {
                    LOGGER.error("Failed to emit OpenLineage event: ", throwable);
                    return null;
                });
    }

    public boolean isEnabled() {
        return openLineageClient != null;
    }

    public String getClientVersion() {
        Package pkg = OpenLineageClient.class.getPackage();
        if (pkg != null) {
            String version = pkg.getImplementationVersion();
            return version != null ? version : NOT_AVAILABLE;
        }
        return NOT_AVAILABLE;
    }

    public URI getProducer() {
        String gitTag = Module.version().contains(SNAPSHOT) ? MAIN_BRANCH_NAME : String.format(VERSION_FORMAT, Module.version());
        return URI.create(String.format(OPEN_LINEAGE_PRODUCER_URI_FORMAT, gitTag));
    }
}
