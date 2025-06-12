/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static io.debezium.connector.postgresql.TestHelper.decoderPlugin;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.openlineage.DebeziumTestTransport;
import io.debezium.openlineage.facets.DebeziumConfigFacet;
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
    public void shouldProduceOpenLineageStartEvent() {

        DebeziumTestTransport debeziumTestTransport = getDebeziumTestTransport();
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.INITIAL.getValue())
                .with("openlineage.integration.enabled", true)
                .with("openlineage.integration.config.file.path", getClass().getClassLoader().getResource("openlineage/openlineage.yml").getPath())
                .with("openlineage.integration.job.description", "This connector does cdc for products")
                .with("openlineage.integration.job.tags", "env=prod,team=cdc")
                .with("openlineage.integration.job.owners", "Mario=maintainer,John Doe=Data scientist");

        start(PostgresConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        Optional<OpenLineage.RunEvent> runEvent = debeziumTestTransport.getRunEvents().stream()
                .filter(e -> e.getEventType() == OpenLineage.RunEvent.EventType.START)
                .findFirst();

        assertThat(runEvent).isPresent();

        assertEventContainsExpectedData(runEvent.get());

    }

    @Test
    public void shouldProduceOpenLineageRunningEvent() {

        DebeziumTestTransport debeziumTestTransport = getDebeziumTestTransport();
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.INITIAL.getValue())
                .with("openlineage.integration.enabled", true)
                .with("openlineage.integration.config.file.path", getClass().getClassLoader().getResource("openlineage/openlineage.yml").getPath())
                .with("openlineage.integration.job.description", "This connector does cdc for products")
                .with("openlineage.integration.job.tags", "env=prod,team=cdc")
                .with("openlineage.integration.job.owners", "Mario=maintainer,John Doe=Data scientist");

        start(PostgresConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        Optional<OpenLineage.RunEvent> runEvent = debeziumTestTransport.getRunEvents().stream()
                .filter(e -> e.getEventType() == OpenLineage.RunEvent.EventType.RUNNING)
                .findFirst();

        assertThat(runEvent).isPresent();

        assertEventContainsExpectedData(runEvent.get());

    }

    @Test
    public void shouldProduceOpenLineageCompleteEvent() {

        DebeziumTestTransport debeziumTestTransport = getDebeziumTestTransport();
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.INITIAL.getValue())
                .with("openlineage.integration.enabled", true)
                .with("openlineage.integration.config.file.path", getClass().getClassLoader().getResource("openlineage/openlineage.yml").getPath())
                .with("openlineage.integration.job.description", "This connector does cdc for products")
                .with("openlineage.integration.job.tags", "env=prod,team=cdc")
                .with("openlineage.integration.job.owners", "Mario=maintainer,John Doe=Data scientist");

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
                .with("openlineage.integration.enabled", true)
                .with("openlineage.integration.config.file.path", getClass().getClassLoader().getResource("openlineage/openlineage.yml").getPath())
                .with("openlineage.integration.job.description", "This connector does cdc for products")
                .with("openlineage.integration.job.tags", "env=prod,team=cdc")
                .with("openlineage.integration.job.owners", "Mario=maintainer,John Doe=Data scientist");

        start(PostgresConnector.class, configBuilder.build());

        waitForSnapshotToBeCompleted("postgres", TestHelper.TEST_SERVER);

        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(() -> debeziumTestTransport.getRunEvents().size() == 6);

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
                .with("openlineage.integration.enabled", true)
                .with("openlineage.integration.config.file.path", getClass().getClassLoader().getResource("openlineage/openlineage.yml").getPath())
                .with("openlineage.integration.job.description", "This connector does cdc for products")
                .with("openlineage.integration.job.tags", "env=prod,team=cdc")
                .with("openlineage.integration.job.owners", "Mario=maintainer,John Doe=Data scientist");

        start(PostgresConnector.class, configBuilder.build());

        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        TestHelper.execute("ALTER TABLE s1.a ADD COLUMN bb VARCHAR(255);");
        TestHelper.execute("INSERT INTO s1.a (aa, bb) VALUES (2, 'test');");

        waitForAvailableRecords();

        List<OpenLineage.RunEvent> runningEvents = debeziumTestTransport.getRunEvents().stream()
                .filter(e -> e.getEventType() == OpenLineage.RunEvent.EventType.RUNNING)
                .toList();

        int expected = decoderPlugin() == PostgresConnectorConfig.LogicalDecoder.PGOUTPUT ? 6 : 7;
        assertThat(runningEvents).hasSize(expected);

        String pkValue = decoderPlugin() == PostgresConnectorConfig.LogicalDecoder.PGOUTPUT ? "int4" : "serial";
        assertCorrectInputDataset(runningEvents.get(1).getInputs(), "s1.a", List.of("pk;serial", "aa;int4"));
        assertCorrectInputDataset(runningEvents.get(2).getInputs(), "s2.a", List.of("pk;serial", "aa;int4"));
        assertCorrectInputDataset(runningEvents.get(5).getInputs(), "s1.a", List.of("pk;" + pkValue, "aa;int4", "bb;varchar"));
    }

    @Test
    public void shouldProduceOpenLineageFailEvent() {

        DebeziumTestTransport debeziumTestTransport = getDebeziumTestTransport();
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.INITIAL.getValue())
                .with(PostgresConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE, "s1.a")
                .with(PostgresConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE.name() + ".s1.a", "this is intentionally a wrong statement")
                .with(CommonConnectorConfig.MAX_RETRIES_ON_ERROR, 1)
                .with(CommonConnectorConfig.RETRIABLE_RESTART_WAIT, 1000)
                .with("openlineage.integration.enabled", true)
                .with("openlineage.integration.config.file.path", OpenLineageIT.class.getClassLoader().getResource("openlineage/openlineage.yml").getPath())
                .with("openlineage.integration.job.description", "This connector does cdc for products")
                .with("openlineage.integration.job.tags", "env=prod,team=cdc")
                .with("openlineage.integration.job.owners", "Mario=maintainer,John Doe=Data scientist");

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

    @Test
    public void shouldProduceOpenLineageOutputDataset() throws Exception {

        TestHelper.execute(SETUP_TABLES_STMT);

        DebeziumTestTransport debeziumTestTransport = getDebeziumTestTransport();
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.INITIAL.getValue())
                .with("schema.history.internal.kafka.bootstrap.servers", "test-kafka:9092")
                .with("openlineage.integration.enabled", true)
                .with("openlineage.integration.config.file.path", getClass().getClassLoader().getResource("openlineage/openlineage.yml").getPath())
                .with("openlineage.integration.job.description", "This connector does cdc for products")
                .with("openlineage.integration.job.tags", "env=prod,team=cdc")
                .with("openlineage.integration.job.owners", "Mario=maintainer,John Doe=Data scientist")
                .with("transforms", "openlineage")
                .with("transforms.openlineage.type", "io.debezium.transforms.openlineage.OpenLineage");

        start(PostgresConnector.class, configBuilder.build());

        waitForSnapshotToBeCompleted("postgres", TestHelper.TEST_SERVER);

        waitForAvailableRecords();

        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(() -> debeziumTestTransport.getRunEvents().size() == 8);

        List<OpenLineage.RunEvent> runningEvents = debeziumTestTransport.getRunEvents().stream()
                .filter(e -> e.getEventType() == OpenLineage.RunEvent.EventType.RUNNING)
                .toList();

        assertThat(runningEvents).hasSize(7);

        assertCorrectOutputDataset(runningEvents.get(5).getOutputs(), "test_server.s1.a", List.of("before;STRUCT",
                "before.pk;INT32",
                "before.aa;INT32",
                "after;STRUCT",
                "after.pk;INT32",
                "after.aa;INT32",
                "source;STRUCT",
                "source.version;STRING",
                "source.connector;STRING",
                "source.name;STRING",
                "source.ts_ms;INT64",
                "source.snapshot;STRING",
                "source.db;STRING",
                "source.sequence;STRING",
                "source.ts_us;INT64",
                "source.ts_ns;INT64",
                "source.schema;STRING",
                "source.table;STRING",
                "source.txId;INT64",
                "source.lsn;INT64",
                "source.xmin;INT64",
                "transaction;STRUCT",
                "transaction.id;STRING",
                "transaction.total_order;INT64",
                "transaction.data_collection_order;INT64",
                "op;STRING",
                "ts_ms;INT64",
                "ts_us;INT64",
                "ts_ns;INT64"));
        assertCorrectOutputDataset(runningEvents.get(6).getOutputs(), "test_server.s2.a", List.of("before;STRUCT",
                "before.pk;INT32",
                "before.aa;INT32",
                "after;STRUCT",
                "after.pk;INT32",
                "after.aa;INT32",
                "source;STRUCT",
                "source.version;STRING",
                "source.connector;STRING",
                "source.name;STRING",
                "source.ts_ms;INT64",
                "source.snapshot;STRING",
                "source.db;STRING",
                "source.sequence;STRING",
                "source.ts_us;INT64",
                "source.ts_ns;INT64",
                "source.schema;STRING",
                "source.table;STRING",
                "source.txId;INT64",
                "source.lsn;INT64",
                "source.xmin;INT64",
                "transaction;STRUCT",
                "transaction.id;STRING",
                "transaction.total_order;INT64",
                "transaction.data_collection_order;INT64",
                "op;STRING",
                "ts_ms;INT64",
                "ts_us;INT64",
                "ts_ns;INT64"));

    }

    private static void assertCorrectInputDataset(List<OpenLineage.InputDataset> inputs, String expectedName, List<String> expectedFields) {
        assertThat(inputs).hasSize(1);
        assertThat(inputs.get(0).getName()).isEqualTo(expectedName);
        assertThat(inputs.get(0).getNamespace()).isEqualTo("postgres://localhost:5432");
        List<OpenLineage.SchemaDatasetFacetFields> tableFields = inputs.get(0).getFacets().getSchema().getFields();
        List<String> actualFields = tableFields.stream().map(f -> String.format("%s;%s", f.getName(), f.getType())).toList();
        assertThat(actualFields).containsAll(expectedFields);
    }

    private static void assertCorrectOutputDataset(List<OpenLineage.OutputDataset> outputs, String expectedName, List<String> expectedFields) {
        assertThat(outputs).hasSize(1);
        assertThat(outputs.get(0).getName()).isEqualTo(expectedName);
        assertThat(outputs.get(0).getNamespace()).isEqualTo("kafka://test-kafka:9092");
        List<OpenLineage.SchemaDatasetFacetFields> tableFields = outputs.get(0).getFacets().getSchema().getFields();
        List<String> actualFields = flattenFields(tableFields);
        assertThat(actualFields).containsAll(expectedFields);
    }

    private static List<String> flattenFields(List<OpenLineage.SchemaDatasetFacetFields> fields) {
        return fields.stream()
                .flatMap(field -> flattenField(field, ""))
                .toList();
    }

    private static Stream<String> flattenField(OpenLineage.SchemaDatasetFacetFields field, String prefix) {
        String currentFieldName = prefix.isEmpty() ? field.getName() : prefix + "." + field.getName();
        String fieldInfo = String.format("%s;%s", currentFieldName, field.getType());

        if (field.getFields() != null && !field.getFields().isEmpty()) {
            return Stream.concat(
                    Stream.of(fieldInfo),
                    field.getFields().stream()
                            .flatMap(nestedField -> flattenField(nestedField, currentFieldName)));
        }

        return Stream.of(fieldInfo);
    }

    // Alternative version that checks structure hierarchically instead of flattening
    private static void assertCorrectOutputDatasetHierarchical(List<OpenLineage.OutputDataset> outputs, String expectedName, Map<String, Object> expectedStructure) {
        assertThat(outputs).hasSize(1);
        assertThat(outputs.get(0).getName()).isEqualTo(expectedName);
        assertThat(outputs.get(0).getNamespace()).isEqualTo("kafka://test-kafka:9092");
        List<OpenLineage.SchemaDatasetFacetFields> tableFields = outputs.get(0).getFacets().getSchema().getFields();
        assertFieldStructure(tableFields, expectedStructure);
    }

    @SuppressWarnings("unchecked")
    private static void assertFieldStructure(List<OpenLineage.SchemaDatasetFacetFields> actualFields, Map<String, Object> expectedStructure) {
        for (Map.Entry<String, Object> expected : expectedStructure.entrySet()) {
            String fieldName = expected.getKey();
            Object fieldSpec = expected.getValue();

            OpenLineage.SchemaDatasetFacetFields actualField = actualFields.stream()
                    .filter(f -> f.getName().equals(fieldName))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Field not found: " + fieldName));

            if (fieldSpec instanceof String) {
                // Simple field: just check type
                assertThat(actualField.getType()).isEqualTo(fieldSpec);
                assertThat(actualField.getFields()).isNullOrEmpty();
            }
            else if (fieldSpec instanceof Map) {
                // Nested field: check type and recurse
                Map<String, Object> nestedSpec = (Map<String, Object>) fieldSpec;
                String expectedType = (String) nestedSpec.get("type");
                Map<String, Object> expectedNestedFields = (Map<String, Object>) nestedSpec.get("fields");

                assertThat(actualField.getType()).isEqualTo(expectedType);
                if (expectedNestedFields != null && !expectedNestedFields.isEmpty()) {
                    assertThat(actualField.getFields()).isNotEmpty();
                    assertFieldStructure(actualField.getFields(), expectedNestedFields);
                }
            }
        }
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
        assertThat(startEvent.getJob().getName()).isEqualTo("test_server.0");
        assertThat(startEvent.getJob().getFacets().getDocumentation().getDescription()).isEqualTo("This connector does cdc for products");

        assertThat(startEvent.getRun().getFacets().getProcessing_engine().getName()).isEqualTo("Debezium");
        assertThat(startEvent.getRun().getFacets().getProcessing_engine().getVersion()).matches("^\\d+\\.\\d+\\.\\d+(\\.Final|-SNAPSHOT)$");
        assertThat(startEvent.getRun().getFacets().getProcessing_engine().getOpenlineageAdapterVersion()).matches("^\\d+\\.\\d+\\.\\d+$");

        DebeziumConfigFacet debeziumConfigFacet = (DebeziumConfigFacet) startEvent.getRun().getFacets().getAdditionalProperties().get("debezium_config");

        assertThat(debeziumConfigFacet.getConfigs()).contains(
                "connector.class=io.debezium.connector.postgresql.PostgresConnector",
                "database.dbname=postgres",
                "database.hostname=localhost",
                "database.password=postgres",
                "database.port=5432",
                "database.sslmode=disable",
                "database.user=postgres",
                "errors.max.retries=-1",
                "errors.retry.delay.initial.ms=300",
                "errors.retry.delay.max.ms=10000",
                "key.converter=org.apache.kafka.connect.json.JsonConverter",
                "name=testing-connector",
                "offset.flush.interval.ms=0",
                "offset.flush.timeout.ms=5000",
                "offset.storage=org.apache.kafka.connect.storage.FileOffsetBackingStore",
                "openlineage.integration.enabled=true",
                "openlineage.integration.job.description=This connector does cdc for products",
                "openlineage.integration.job.owners=Mario=maintainer,John Doe=Data scientist",
                "openlineage.integration.job.tags=env=prod,team=cdc",
                "record.processing.order=ORDERED",
                "record.processing.shutdown.timeout.ms=1000",
                "record.processing.threads=",
                "record.processing.with.serial.consumer=false",
                "slot.max.retries=2",
                "slot.retry.delay.ms=2000",
                "snapshot.mode=initial",
                "status.update.interval.ms=100",
                "topic.prefix=test_server",
                "value.converter=org.apache.kafka.connect.json.JsonConverter")
                .anyMatch(config -> config.startsWith("openlineage.integration.config.file.path=") && config.contains("openlineage.yml"))
                .anyMatch(config -> config.startsWith("offset.storage.file.filename=") && config.contains("file-connector-offsets.txt"));

        Map<String, String> tags = startEvent.getJob().getFacets().getTags().getTags()
                .stream()
                .collect(Collectors.toMap(
                        OpenLineage.TagsJobFacetFields::getKey,
                        OpenLineage.TagsJobFacetFields::getValue));

        assertThat(startEvent.getProducer().toString()).startsWith("https://github.com/debezium/debezium/");
        assertThat(tags).contains(entry("env", "prod"), entry("team", "cdc"));

        Map<String, String> ownership = startEvent.getJob().getFacets().getOwnership().getOwners()
                .stream()
                .collect(Collectors.toMap(
                        OpenLineage.OwnershipJobFacetOwners::getName,
                        OpenLineage.OwnershipJobFacetOwners::getType));

        assertThat(ownership).contains(entry("Mario", "maintainer"), entry("John Doe", "Data scientist"));
    }
}
