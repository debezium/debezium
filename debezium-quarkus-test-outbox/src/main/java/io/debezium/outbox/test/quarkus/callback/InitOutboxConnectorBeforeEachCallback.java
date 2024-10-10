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

import io.debezium.outbox.test.quarkus.JdbcDataSourceSourceConnector;
import io.debezium.outbox.test.quarkus.remote.DebeziumConnectorConfiguration;
import io.debezium.outbox.test.quarkus.remote.DebeziumConnectorConfigurationConfig;
import io.debezium.outbox.test.quarkus.remote.RemoteConnectorApi;
import io.quarkus.test.junit.callback.QuarkusTestBeforeEachCallback;
import io.quarkus.test.junit.callback.QuarkusTestMethodContext;

/**
 * A Quarkus test callback that automatically registers a Debezium outbox connector
 * before each test method execution.
 *
 * <p>
 * This class handles the registration of the Debezium outbox connector by retrieving
 * the necessary database connection configuration from the Quarkus application
 * properties. It then configures the connector to capture changes from the PostgreSQL
 * database and streams those changes to Kafka topics.
 * </p>
 *
 * <p>
 * The connector registration process waits for up to 30 seconds for the connector
 * to start successfully before proceeding with the test execution. If the registration
 * fails or the connector does not reach the "RUNNING" state within the timeout,
 * an exception is thrown.
 * </p>
 *
 * @see io.debezium.outbox.test.quarkus.remote.RemoteConnectorApi
 */
public final class InitOutboxConnectorBeforeEachCallback implements QuarkusTestBeforeEachCallback {

    /**
     * Registers the Debezium outbox connector before each test method execution.
     *
     * <p>
     * This method retrieves the database connection URL from the Quarkus configuration.
     * It supports both JDBC and reactive data sources, adjusts the hostname if running
     * in a Docker environment, and registers the Debezium outbox connector using
     * {@link io.debezium.outbox.test.quarkus.remote.RemoteConnectorApi}.
     * </p>
     *
     * <p>
     * The connector configuration includes database connection details such as hostname,
     * port, database name, username, and password. Once the connector is registered, the
     * method waits for up to 30 seconds to ensure that the connector is running
     * successfully before allowing the test method to proceed.
     * </p>
     *
     * @param context The test method context provided by the Quarkus test framework.
     * @throws RuntimeException if the connector registration or startup fails.
     */
    // https://github.com/debezium/debezium-examples/blob/main/outbox/register-postgres.json
    @Override
    public void beforeEach(final QuarkusTestMethodContext context) {
        try {
            // Get the JDBC data source type from Quarkus configuration
            final JdbcDataSourceSourceConnector jdbcDataSourceSourceConnector = JdbcDataSourceSourceConnector
                    .fromDbKind(ConfigProvider.getConfig().getValue("quarkus.datasource.db-kind", String.class));

            // Get the database connection URL from Quarkus configuration
            final String jdbcUrl = Stream.of("quarkus.datasource.jdbc.url", "quarkus.datasource.reactive.url")
                    .map(propertyName -> ConfigProvider.getConfig().getOptionalValue(propertyName, String.class))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .findFirst()
                    .map(url -> url.replace("vertx-reactive:", ""))
                    .map(url -> url.replace("localhost", "host.docker.internal"))
                    .orElseThrow(() -> new IllegalStateException("Could not find quarkus.datasource.jdbc.url or quarkus.datasource.reactive.url."));

            // Parse the database connection details from the URL
            final URI uri = new URI(jdbcUrl.substring(5));
            final String databaseHostname = uri.getHost();
            final int port = uri.getPort();
            final String databaseDbname = uri.getPath().substring(1); // Remove leading "/"

            // Create an instance of the RemoteConnectorApi
            final RemoteConnectorApi remoteConnectorApi = RemoteConnectorApi.createInstance();
            remoteConnectorApi.registerOutboxConnector(
                    DebeziumConnectorConfiguration.newBuilder()
                            .withName(RemoteConnectorApi.CONNECTOR_NAME)
                            .withConfig(DebeziumConnectorConfigurationConfig.newBuilder()
                                    .withJdbcDataSourceSourceConnector(jdbcDataSourceSourceConnector)
                                    .withDatabaseHostname(databaseHostname)
                                    .withDatabasePort(port)
                                    .withDatabaseDbname(databaseDbname)
                                    .withDatabaseUser(ConfigProvider.getConfig().getValue("quarkus.datasource.username", String.class))
                                    .withDatabasePassword(ConfigProvider.getConfig().getValue("quarkus.datasource.password", String.class))
                                    .withDatabaseServerName(databaseDbname)
                                    .build())
                            .build());

            // Wait for the connector to reach the "RUNNING" state
            await().atMost(30, TimeUnit.SECONDS).until(() -> remoteConnectorApi.outboxConnectorStatus().isRunning());
        }
        catch (final URISyntaxException uriSyntaxException) {
            throw new RuntimeException(uriSyntaxException);
        }
    }
}
