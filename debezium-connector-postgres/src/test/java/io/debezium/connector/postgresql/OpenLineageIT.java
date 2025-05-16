/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.openlineage.DebeziumConfigFacet;
import io.debezium.openlineage.DebeziumTestTransport;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.transports.TransportBuilder;

public class OpenLineageIT extends AbstractAsyncEngineConnectorTest {

    private static final String INSERT_STMT = "INSERT INTO s1.a (aa) VALUES (1);" +
            "INSERT INTO s2.a (aa) VALUES (2);" +
            "INSERT INTO s1.a (aa) VALUES (3);" +
            "INSERT INTO s2.a (aa) VALUES (4);";
    private static final String CREATE_TABLES_STMT = "DROP SCHEMA IF EXISTS s1 CASCADE;" +
            "DROP SCHEMA IF EXISTS s2 CASCADE;" +
            "CREATE SCHEMA s1; " +
            "CREATE SCHEMA s2; " +
            "CREATE TABLE s1.a (pk SERIAL NOT NULL PRIMARY KEY, aa integer);" +
            "CREATE TABLE s2.a (pk SERIAL NOT NULL PRIMARY KEY, aa integer);";
    private static final String SETUP_TABLES_STMT = CREATE_TABLES_STMT + INSERT_STMT;

    @Before
    public void before() {
        initializeConnectorTestFramework();
        getDebeziumTestTransport().clear();
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

        Optional<OpenLineage.RunEvent> runEvent = debeziumTestTransport.getRunEvents().stream()
                .filter(e -> e.getEventType() == OpenLineage.RunEvent.EventType.START)
                .findFirst();

        assertThat(runEvent).isPresent();

        assertEventContainsExpectedData(runEvent.get());

    }

    @Test
    public void shouldProduceOpenLineageRunningEvent() throws Exception {
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

        Optional<OpenLineage.RunEvent> runEvent = debeziumTestTransport.getRunEvents().stream()
                .filter(e -> e.getEventType() == OpenLineage.RunEvent.EventType.RUNNING)
                .findFirst();

        assertThat(runEvent).isPresent();

        assertEventContainsExpectedData(runEvent.get());

    }

    @Test
    public void shouldProduceOpenLineageCompleteEvent() throws Exception {
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

        stopConnector(b -> {
            Optional<OpenLineage.RunEvent> runEvent = debeziumTestTransport.getRunEvents().stream()
                    .filter(e -> e.getEventType() == OpenLineage.RunEvent.EventType.COMPLETE)
                    .findFirst();

            assertThat(runEvent).isPresent();

            assertEventContainsExpectedData(runEvent.get());
        });

    }

    @Test
    public void shouldProduceOpenLineageInputDataset() throws Exception {

        TestHelper.execute(SETUP_TABLES_STMT);

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

        waitForSnapshotToBeCompleted("postgres", TestHelper.TEST_SERVER);

        List<OpenLineage.RunEvent> runningEvents = debeziumTestTransport.getRunEvents().stream()
                .filter(e -> e.getEventType() == OpenLineage.RunEvent.EventType.RUNNING)
                .toList();

        assertThat(runningEvents).hasSize(5);

        assertEventContainsExpectedData(runningEvents.get(0));
        assertEventContainsExpectedData(runningEvents.get(1));

        assertCorrectInputDataset(runningEvents.get(1).getInputs(), "s1.a", List.of("pk;serial", "aa;int4"));
        assertCorrectInputDataset(runningEvents.get(2).getInputs(), "s2.a", List.of("pk;serial", "aa;int4"));

    }

    @Test
    public void shouldProduceOpenLineageInputDatasetUponDDLEvent() throws Exception {

        TestHelper.execute(SETUP_TABLES_STMT);

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

        waitForSnapshotToBeCompleted("postgres", TestHelper.TEST_SERVER);

        TestHelper.execute("ALTER TABLE s1.a ADD COLUMN bb VARCHAR(255);");
        TestHelper.execute("INSERT INTO s1.a (aa, bb) VALUES (2, 'test');");

        waitForAvailableRecords();

        List<OpenLineage.RunEvent> runningEvents = debeziumTestTransport.getRunEvents().stream()
                .filter(e -> e.getEventType() == OpenLineage.RunEvent.EventType.RUNNING)
                .toList();

        assertThat(runningEvents).hasSize(7);

        assertCorrectInputDataset(runningEvents.get(1).getInputs(), "s1.a", List.of("pk;serial", "aa;int4"));
        assertCorrectInputDataset(runningEvents.get(2).getInputs(), "s2.a", List.of("pk;serial", "aa;int4"));
        assertCorrectInputDataset(runningEvents.get(5).getInputs(), "s1.a", List.of("pk;serial", "aa;int4", "bb;varchar"));
    }

    @Test
    public void shouldProduceOpenLineageFailEvent() throws Exception {
        // TestHelper.execute(SETUP_TABLES_STMT);

        DebeziumTestTransport debeziumTestTransport = getDebeziumTestTransport();
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.INITIAL.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .with(PostgresConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE, "s1.a")
                .with(PostgresConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE.name() + ".s1.a", "this is intentionally a wrong statement")
                .with(CommonConnectorConfig.MAX_RETRIES_ON_ERROR, 1)
                .with(CommonConnectorConfig.RETRIABLE_RESTART_WAIT, 1000)
                .with("openlineage.integration.enabled", true)
                .with("openlineage.integration.config.path", getClass().getClassLoader().getResource("openlineage/openlineage.yml").getPath())
                .with("openlineage.integration.job.description", "This connector does cdc for products")
                .with("openlineage.integration.tags", "env=prod,team=cdc")
                .with("openlineage.integration.owners", "Mario=maintainer,John Doe=Data scientist");

        AtomicReference<List<OpenLineage.RunEvent>> result = new AtomicReference<>(null);
        start(PostgresConnector.class, configBuilder.build(), (success, message, error) -> {
            result.set(debeziumTestTransport.getRunEvents());
        });

        Awaitility.await().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
            List<OpenLineage.RunEvent> events = result.get();

            assertThat(events)
                    .as("Wait for result to be set")
                    .isNotNull();

            Optional<OpenLineage.RunEvent> runEvent = events.stream()
                    .filter(e -> e.getEventType() == OpenLineage.RunEvent.EventType.FAIL)
                    .findFirst();

            assertThat(runEvent)
                    .as("Expect at least one FAIL event")
                    .isPresent();

            assertThat(runEvent.get().getRun().getFacets().getErrorMessage()).isNotNull();
            assertThat(runEvent.get().getRun().getFacets().getErrorMessage().getMessage())
                    .isEqualTo("java.util.concurrent.ExecutionException: org.apache.kafka.connect.errors.ConnectException: Snapshotting of table s1.a failed");
            assertThat(runEvent.get().getRun().getFacets().getErrorMessage().getStackTrace()).contains("Caused by: org.postgresql.util.PSQLException:");

        });
    }

    private static void assertCorrectInputDataset(List<OpenLineage.InputDataset> inputs, String expectedTableName, List<String> expectedFields) {
        assertThat(inputs).hasSize(1);
        assertThat(inputs.get(0).getName()).isEqualTo(expectedTableName);
        assertThat(inputs.get(0).getNamespace()).isEqualTo("postgres://localhost:5432");
        List<OpenLineage.SchemaDatasetFacetFields> tableFields = inputs.get(0).getFacets().getSchema().getFields();
        List<String> actualFields = tableFields.stream().map(f -> String.format("%s;%s", f.getName(), f.getType())).toList();
        assertThat(actualFields).containsAll(expectedFields);
    }

    private static DebeziumTestTransport getDebeziumTestTransport() {
        ServiceLoader<TransportBuilder> loader = ServiceLoader.load(TransportBuilder.class);
        Optional<TransportBuilder> optionalBuilder = StreamSupport.stream(loader.spliterator(), false)
                .filter(b -> b.getType().equals("debezium"))
                .findFirst();

        return (DebeziumTestTransport) optionalBuilder.orElseThrow(
                () -> new IllegalArgumentException("Failed to find TransportBuilder")).build(null);
    }

    private static void assertEventContainsExpectedData(OpenLineage.RunEvent startEvent) {

        assertThat(startEvent.getJob().getNamespace()).isEqualTo("test_server");
        assertThat(startEvent.getJob().getName()).isEqualTo("testing-connector");
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
}
