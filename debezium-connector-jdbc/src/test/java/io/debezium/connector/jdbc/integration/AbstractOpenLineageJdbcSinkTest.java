/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.StreamSupport;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.openlineage.DebeziumTestTransport;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.transports.TransportBuilder;

/**
 * Abstract OpenLineage JDBC Sink Connector integration test.
 *
 * @author Mario Fiore Vitale
 */
public abstract class AbstractOpenLineageJdbcSinkTest extends AbstractJdbcSinkTest {

    public AbstractOpenLineageJdbcSinkTest(Sink sink) {
        super(sink);
    }

    @AfterEach
    public void afterEach() {
        super.afterEach();

        getDebeziumTestTransport().clear();
    }

    protected Map<String, String> getDefaultSinkConfig() {
        Map<String, String> defaultSinkConfig = super.getDefaultSinkConfig();
        defaultSinkConfig.put("name", "my-sink-with-lineage");
        defaultSinkConfig.put("openlineage.integration.enabled", "true");
        defaultSinkConfig.put("openlineage.integration.config.file.path", getClass().getClassLoader().getResource("openlineage/openlineage.yml").getPath());
        defaultSinkConfig.put("openlineage.integration.job.description", "This connector does cdc for products");
        defaultSinkConfig.put("openlineage.integration.job.tags", "env=prod,team=cdc");
        defaultSinkConfig.put("openlineage.integration.job.owners", "Mario=maintainer,John Doe=Data scientist");

        return defaultSinkConfig;
    }

    @Test
    public void shouldProduceOpenLineageStartEvent() {

        final Map<String, String> properties = getDefaultSinkConfig();

        DebeziumTestTransport debeziumTestTransport = getDebeziumTestTransport();

        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        Optional<OpenLineage.RunEvent> runEvent = debeziumTestTransport.getRunEvents().stream()
                .filter(e -> e.getEventType() == OpenLineage.RunEvent.EventType.START)
                .findFirst();

        Assertions.assertThat(runEvent).isPresent();

        assertEventContainsExpectedData(runEvent.get());
    }

    @Test
    public void shouldProduceOpenLineageCompleteEvent() {

        final Map<String, String> properties = getDefaultSinkConfig();

        DebeziumTestTransport debeziumTestTransport = getDebeziumTestTransport();

        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        stopSinkConnector();

        Optional<OpenLineage.RunEvent> runEvent = debeziumTestTransport.getRunEvents().stream()
                .filter(e -> e.getEventType() == OpenLineage.RunEvent.EventType.COMPLETE)
                .findFirst();

        assertThat(runEvent).isPresent();
        assertEventContainsExpectedData(runEvent.get());

    }

    protected abstract void assertEventContainsExpectedData(OpenLineage.RunEvent runEvent);

    private static DebeziumTestTransport getDebeziumTestTransport() {
        ServiceLoader<TransportBuilder> loader = ServiceLoader.load(TransportBuilder.class);
        Optional<TransportBuilder> optionalBuilder = StreamSupport.stream(loader.spliterator(), false)
                .filter(b -> b.getType().equals("debezium"))
                .findFirst();

        return (DebeziumTestTransport) optionalBuilder.orElseThrow(
                () -> new IllegalArgumentException("Failed to find TransportBuilder")).build(null);
    }
}
