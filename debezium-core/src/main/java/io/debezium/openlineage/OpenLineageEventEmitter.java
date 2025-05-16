/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage;

import java.net.URI;
import java.nio.file.Path;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.Module;
import io.debezium.config.Configuration;
import io.openlineage.client.Clients;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.OpenLineageClientException;

public class OpenLineageEventEmitter {

    private static final Logger LOGGER = LoggerFactory.getLogger(OpenLineageEventEmitter.class);

    private static final String OPEN_LINEAGE_PRODUCER_URI_FORMAT = "https://github.com/debezium/debezium/%s";

    private final OpenLineageClient openLineageClient;

    public OpenLineageEventEmitter(Configuration config) {

        // TODO Move configs to CommonConnectorConfig
        if (config.getBoolean("openlineage.integration.enabled", false)) {
            openLineageClient = Clients.newClient(() -> List.of(
                    Path.of(config.getString("openlineage.integration.config.path", "."))));
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
        try {
            openLineageClient.emit(event);
        }
        catch (OpenLineageClientException exception) {
            LOGGER.error("Failed to emit OpenLineage event: ", exception);
        }
    }

    public boolean isEnabled() {
        return openLineageClient != null;
    }

    public String getClientVersion() {
        Package pkg = OpenLineageClient.class.getPackage();
        if (pkg != null) {
            String version = pkg.getImplementationVersion();
            return version != null ? version : "N/A";
        }
        return "N/A";
    }

    public URI getProducer() {
        String gitTag = Module.version().contains("SNAPSHOT") ? "main" : String.format("v%s", Module.version());
        return URI.create(String.format(OPEN_LINEAGE_PRODUCER_URI_FORMAT, gitTag));
    }
}
