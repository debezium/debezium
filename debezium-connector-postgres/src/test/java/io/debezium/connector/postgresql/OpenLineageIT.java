/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.openlineage.DebeziumConfigFacet;
import io.debezium.openlineage.DebeziumTestTransport;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.transports.TransportBuilder;

public class OpenLineageIT extends AbstractAsyncEngineConnectorTest {

    @Before
    public void before() {
        initializeConnectorTestFramework();
    }

    @After
    public void after() {
        stopConnector();
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();
    }

    @Test
    public void shouldProduceOpenLineageStartEvent() throws Exception {
        // TestHelper.execute(SETUP_TABLES_STMT);

        DebeziumTestTransport debeziumTestTransport = getDebeziumTestTransport();
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.INITIAL.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .with("openlineage.integration.enabled", true)
                .with("openlineage.integration.config.path", getClass().getClassLoader().getResource("openlineage/openlineage.yml").getPath())
                .with("openlineage.integration.job.description", "This connector does cdc for products")
                .with("openlineage.integration.tags", "env=prod,team=cdc")
                .with("openlineage.integration.owners", "Mario=maintainer,John Doe=Data scientist");

        start(PostgresConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        assertThat(debeziumTestTransport.getRunEvents().size()).isEqualTo(1);
        assertThat(debeziumTestTransport.getJobEvents().size()).isEqualTo(0);
        assertThat(debeziumTestTransport.getDatasetEvents().size()).isEqualTo(0);

        OpenLineage.RunEvent startEvent = debeziumTestTransport.getRunEvents().get(0);

        assertThat(startEvent.getJob().getNamespace()).isEqualTo("test_server");
        assertThat(startEvent.getJob().getName()).isEqualTo("test_server");
        assertThat(startEvent.getJob().getFacets().getDocumentation().getDescription()).isEqualTo("This connector does cdc for products");

        assertThat(startEvent.getRun().getFacets().getProcessing_engine().getName()).isEqualTo("Debezium");
        assertThat(startEvent.getRun().getFacets().getProcessing_engine().getVersion()).matches("^\\d+\\.\\d+\\.\\d+(\\.Final|-SNAPSHOT)$");
        assertThat(startEvent.getRun().getFacets().getProcessing_engine().getOpenlineageAdapterVersion()).matches("^\\d+\\.\\d+\\.\\d+$");

        DebeziumConfigFacet debeziumConfigFacet = (DebeziumConfigFacet) startEvent.getRun().getFacets().getAdditionalProperties().get("debezium_config");

        assertThat(debeziumConfigFacet.getConfigs()).contains(
                entry("connector.class", "io.debezium.connector.postgresql.PostgresConnector"),
                entry("database.dbname", "postgres"),
                entry("database.hostname", "localhost"),
                entry("database.password", "postgres"),
                entry("database.port", "5432"),
                entry("database.sslmode", "disable"),
                entry("database.topic.prefix", "dbserver1"),
                entry("database.user", "postgres"),
                entry("errors.max.retries", "-1"),
                entry("errors.retry.delay.initial.ms", "300"),
                entry("errors.retry.delay.max.ms", "10000"),
                entry("internal.task.management.timeout.ms", "180000"),
                entry("key.converter", "org.apache.kafka.connect.json.JsonConverter"),
                entry("name", "testing-connector"),
                entry("offset.flush.interval.ms", "0"),
                entry("offset.flush.timeout.ms", "5000"),
                entry("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore"),
                entry("offset.storage.file.filename", "/home/mvitale/Projects/debezium/debezium-connector-postgres/target/data/file-connector-offsets.txt"),
                entry("openlineage.integration.config.path",
                        "/home/mvitale/Projects/debezium/debezium-connector-postgres/target/test-classes/openlineage/openlineage.yml"),
                entry("openlineage.integration.enabled", "true"),
                entry("openlineage.integration.job.description", "This connector does cdc for products"),
                entry("openlineage.integration.owners", "Mario=maintainer,John Doe=Data scientist"),
                entry("openlineage.integration.tags", "env=prod,team=cdc"),
                entry("plugin.name", "decoderbufs"),
                entry("record.processing.order", "ORDERED"),
                entry("record.processing.shutdown.timeout.ms", "1000"),
                entry("record.processing.threads", ""),
                entry("record.processing.with.serial.consumer", "false"),
                entry("slot.drop.on.stop", "false"),
                entry("slot.max.retries", "2"),
                entry("slot.retry.delay.ms", "2000"),
                entry("snapshot.mode", "initial"),
                entry("status.update.interval.ms", "100"),
                entry("topic.prefix", "test_server"),
                entry("value.converter", "org.apache.kafka.connect.json.JsonConverter"));

        assertThat(startEvent.getProducer().toString()).startsWith("https://github.com/debezium/debezium/");

        Map<String, String> tags = startEvent.getJob().getFacets().getTags().getTags()
                .stream()
                .collect(Collectors.toMap(
                        OpenLineage.TagsJobFacetFields::getKey,
                        OpenLineage.TagsJobFacetFields::getValue));

        assertThat(tags).contains(entry("env", "prod"), entry("team", "cdc"));

        Map<String, String> ownership = startEvent.getJob().getFacets().getOwnership().getOwners()
                .stream()
                .collect(Collectors.toMap(
                        OpenLineage.OwnershipJobFacetOwners::getName,
                        OpenLineage.OwnershipJobFacetOwners::getType));

        assertThat(ownership).contains(entry("Mario", "maintainer"), entry("John Doe", "Data scientist"));

    }

    private static DebeziumTestTransport getDebeziumTestTransport() {
        ServiceLoader<TransportBuilder> loader = ServiceLoader.load(TransportBuilder.class);
        Optional<TransportBuilder> optionalBuilder = StreamSupport.stream(loader.spliterator(), false)
                .filter(b -> b.getType().equals("debezium"))
                .findFirst();

        return (DebeziumTestTransport) optionalBuilder.orElseThrow(
                () -> new IllegalArgumentException("Failed to find TransportBuilder")).build(null);
    }
}
