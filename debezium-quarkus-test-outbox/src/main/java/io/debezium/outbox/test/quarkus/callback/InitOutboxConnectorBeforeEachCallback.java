/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.test.quarkus.callback;

import static org.awaitility.Awaitility.await;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.eclipse.microprofile.config.ConfigProvider;

import io.debezium.outbox.test.quarkus.remote.DebeziumConnectorConfiguration;
import io.debezium.outbox.test.quarkus.remote.DebeziumConnectorConfigurationConfig;
import io.debezium.outbox.test.quarkus.remote.RemoteConnectorApi;
import io.quarkus.test.junit.callback.QuarkusTestBeforeEachCallback;
import io.quarkus.test.junit.callback.QuarkusTestMethodContext;

public final class InitOutboxConnectorBeforeEachCallback implements QuarkusTestBeforeEachCallback {

    // https://github.com/debezium/debezium-examples/blob/main/outbox/register-postgres.json
    @Override
    public void beforeEach(final QuarkusTestMethodContext context) {
        try {
            final String jdbcUrl = Stream.of("quarkus.datasource.jdbc.url", "quarkus.datasource.reactive.url")
                    .map(propertyName -> ConfigProvider.getConfig().getOptionalValue(propertyName, String.class))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .findFirst()
                    .map(url -> url.replace("vertx-reactive:", ""))
                    .map(url -> url.replace("localhost", "host.docker.internal"))
                    .orElseThrow(() -> new IllegalStateException("Could not find quarkus.datasource.jdbc.url or quarkus.datasource.reactive.url."));
            final URI uri = new URI(jdbcUrl.substring(5));
            final String databaseHostname = uri.getHost();
            final int port = uri.getPort();
            final String databaseDbname = uri.getPath().substring(1); // Remove leading "/"
            final RemoteConnectorApi remoteConnectorApi = RemoteConnectorApi.createInstance();
            remoteConnectorApi.registerOutboxConnector(
                    DebeziumConnectorConfiguration.newBuilder()
                            .withName(RemoteConnectorApi.CONNECTOR_NAME)
                            .withConfig(DebeziumConnectorConfigurationConfig.newBuilder()
                                    .withDatabaseHostname(databaseHostname)
                                    .withDatabasePort(port)
                                    .withDatabaseDbname(databaseDbname)
                                    .withDatabaseUser(ConfigProvider.getConfig().getValue("quarkus.datasource.username", String.class))
                                    .withDatabasePassword(ConfigProvider.getConfig().getValue("quarkus.datasource.password", String.class))
                                    .withDatabaseServerName(databaseDbname)
                                    .build())
                            .build());

            await().atMost(30, TimeUnit.SECONDS).until(() -> remoteConnectorApi.outboxConnectorStatus().isRunning());
        }
        catch (final URISyntaxException uriSyntaxException) {
            throw new RuntimeException(uriSyntaxException);
        }
    }
}
