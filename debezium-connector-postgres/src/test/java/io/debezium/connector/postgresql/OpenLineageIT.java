/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.net.URI;
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

        assertThat(startEvent.getProducer()).isEqualTo(URI.create("https://github.com/debezium/debezium"));

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
