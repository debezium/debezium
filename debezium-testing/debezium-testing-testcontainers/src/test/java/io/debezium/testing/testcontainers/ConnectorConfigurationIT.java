/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.node.ObjectNode;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.ContainerConfig;

public class ConnectorConfigurationIT {

    private ObjectMapper mapper = new ObjectMapper();

    @Test
    public void shouldSerializeConnectorConfiguration() throws IOException {
        final ConnectorConfiguration configuration = ConnectorConfiguration.create();
        configuration.with("connector.class", "foo");
        configuration.with("database.hostname", "bar");
        final Connector connector = Connector.from("myconnector", configuration);

        final String json = connector.toJson();

        final ObjectNode connectionNode = mapper.readValue(json, ObjectNode.class);
        assertThat(connectionNode.get("name").asText()).isEqualTo("myconnector");

        final JsonNode configNode = connectionNode.get("config");
        assertThat(configNode.get("connector.class").asText()).isEqualTo("foo");
        assertThat(configNode.get("database.hostname").asText()).isEqualTo("bar");
    }

    @Test
    public void shouldLoadConnectorConfigurationFromFile() throws IOException {
        final InputStream configFile = ConnectorConfigurationIT.class.getClassLoader().getResourceAsStream("config.json");
        final Connector connector = Connector.fromJson(configFile);

        final String json = connector.toJson();

        final ObjectNode connectionNode = mapper.readValue(json, ObjectNode.class);
        assertThat(connectionNode.get("name").asText()).isEqualTo("inventory-connector");

        final JsonNode configNode = connectionNode.get("config");
        assertThat(configNode.get("connector.class").asText()).isEqualTo("io.debezium.connector.mysql.MySqlConnector");
        assertThat(configNode.get("database.hostname").asText()).isEqualTo("192.168.99.100");
    }

    @Test
    public void shouldOverrideConfigurationFromJdbcContainer() throws IOException {
        final ContainerConfig containerConfig = mock(ContainerConfig.class);
        when(containerConfig.getHostName()).thenReturn("localhost");

        final InspectContainerResponse inspectContainerResponse = mock(InspectContainerResponse.class);
        when(inspectContainerResponse.getConfig()).thenReturn(containerConfig);

        final JdbcDatabaseContainer<?> jdbcDatabaseContainer = mock(JdbcDatabaseContainer.class);
        when(jdbcDatabaseContainer.getDriverClassName()).thenReturn("org.postgresql.Driver");
        when(jdbcDatabaseContainer.getDatabaseName()).thenReturn("db");
        when(jdbcDatabaseContainer.getPassword()).thenReturn("");
        when(jdbcDatabaseContainer.getUsername()).thenReturn("");
        when(jdbcDatabaseContainer.getExposedPorts()).thenReturn(Arrays.asList(9090));
        when(jdbcDatabaseContainer.getContainerInfo()).thenReturn(inspectContainerResponse);

        final InputStream configFile = ConnectorConfigurationIT.class.getClassLoader().getResourceAsStream("config.json");
        final Connector connector = Connector.fromJson(configFile);

        connector.appendOrOverrideConfiguration(ConnectorConfiguration.forJdbcContainer(jdbcDatabaseContainer));

        final String json = connector.toJson();

        final ObjectNode connectionNode = mapper.readValue(json, ObjectNode.class);
        assertThat(connectionNode.get("name").asText()).isEqualTo("inventory-connector");

        final JsonNode configNode = connectionNode.get("config");
        assertThat(configNode.get("connector.class").asText()).isEqualTo("io.debezium.connector.postgresql.PostgresConnector");
        assertThat(configNode.get("database.hostname").asText()).isEqualTo("localhost");
        assertThat(configNode.get("database.dbname").asText()).isEqualTo("db");
    }
}
