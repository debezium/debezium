/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import io.debezium.bindings.kafka.KafkaDebeziumSinkRecord;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.junit.jupiter.SinkRecordFactoryArgumentsProvider;
import io.debezium.connector.jdbc.util.SinkRecordFactory;
import io.debezium.openlineage.DebeziumTestTransport;
import io.debezium.sink.SinkConnectorConfig;
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
        defaultSinkConfig.put("openlineage.integration.dataset.kafka.bootstrap.servers", "localhost:9092");

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

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void shouldProduceOpenLineageInputDataset(SinkRecordFactory factory) {

        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, SinkConnectorConfig.PrimaryKeyMode.NONE.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.INSERT.getValue());

        DebeziumTestTransport debeziumTestTransport = getDebeziumTestTransport();

        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final KafkaDebeziumSinkRecord createRecord = factory.createRecordNoKey(topicName);
        consume(createRecord);

        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(() -> debeziumTestTransport.getRunEvents().size() == 3);

        List<OpenLineage.RunEvent> runningEvents = debeziumTestTransport.getRunEvents().stream()
                .filter(e -> e.getEventType() == OpenLineage.RunEvent.EventType.RUNNING)
                .toList();

        assertThat(runningEvents).hasSize(2);

        assertEventContainsExpectedData(runningEvents.get(0));
        assertEventContainsExpectedData(runningEvents.get(1));

        List<String> expectedValues = factory.isFlattened() ? List.of("id;INT8", "name;STRING", "nick_name_;STRING")
                : List.of("before;STRUCT", "after;STRUCT",
                        "source;STRUCT",
                        "op;STRING",
                        "ts_ms;INT64",
                        "ts_us;INT64",
                        "ts_ns;INT64",
                        "transaction;STRUCT");

        assertCorrectInputDataset(runningEvents.get(1).getInputs(),
                "server1.schema." + tableName,
                expectedValues,
                "kafka://localhost:9092");

    }

    private static void assertCorrectInputDataset(List<OpenLineage.InputDataset> inputs, String expectedName, List<String> expectedFields,
                                                  String expectedNamespacePrefix) {
        assertThat(inputs).hasSize(1);
        assertThat(inputs.get(0).getName()).isEqualTo(expectedName);
        assertThat(inputs.get(0).getNamespace()).startsWith(expectedNamespacePrefix);
        List<OpenLineage.SchemaDatasetFacetFields> tableFields = inputs.get(0).getFacets().getSchema().getFields();
        List<String> actualFields = tableFields.stream().map(f -> String.format("%s;%s", f.getName(), f.getType())).toList();
        assertThat(actualFields).containsAll(expectedFields);
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
