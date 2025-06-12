/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.sql.SQLException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.openlineage.DebeziumTestTransport;
import io.debezium.openlineage.facets.DebeziumConfigFacet;
import io.debezium.relational.TableId;
import io.debezium.util.Testing;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.transports.TransportBuilder;

/**
 * Integration test for the Debezium SQL Server connector.
 *
 * @author Mario Fiore Vitale
 */
public class OpenLineageIT extends AbstractAsyncEngineConnectorTest {

    private SqlServerConnection connection;

    @Before
    public void before() throws SQLException {

        TestHelper.createTestDatabases(TestHelper.TEST_DATABASE_1, TestHelper.TEST_DATABASE_2);
        connection = TestHelper.multiPartitionTestConnection();

        TableId db1TableA = new TableId(TestHelper.TEST_DATABASE_1, "dbo", "tableA");
        TableId db1TableB = new TableId(TestHelper.TEST_DATABASE_1, "dbo", "tableB");

        connection.execute(
                "CREATE TABLE %s (id int primary key, colA varchar(32))"
                        .formatted(connection.quotedTableIdString(db1TableA)),
                "CREATE TABLE %s (id int primary key, colB varchar(32))"
                        .formatted(connection.quotedTableIdString(db1TableB)),
                "INSERT INTO %s VALUES(1, 'a1')"
                        .formatted(connection.quotedTableIdString(db1TableA)),
                "INSERT INTO %s VALUES(2, 'b')"
                        .formatted(connection.quotedTableIdString(db1TableB)));
        TestHelper.enableTableCdc(connection, db1TableA);
        TestHelper.enableTableCdc(connection, db1TableB);

        TableId db2TableA = new TableId(TestHelper.TEST_DATABASE_2, "dbo", "tableA");
        TableId db2TableC = new TableId(TestHelper.TEST_DATABASE_2, "dbo", "tableC");

        connection.execute(
                "CREATE TABLE %s (id int primary key, colA varchar(32))"
                        .formatted(connection.quotedTableIdString(db2TableA)),
                "CREATE TABLE %s (id int primary key, colC varchar(32))"
                        .formatted(connection.quotedTableIdString(db2TableC)),
                "INSERT INTO %s VALUES(3, 'a2')"
                        .formatted(connection.quotedTableIdString(db2TableA)),
                "INSERT INTO %s VALUES(4, 'c')"
                        .formatted(connection.quotedTableIdString(db2TableC)));
        TestHelper.enableTableCdc(connection, db2TableA);
        TestHelper.enableTableCdc(connection, db2TableC);

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @After
    public void after() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void shouldProduceMultipleOpenLineageJobRunningEvent() {

        DebeziumTestTransport debeziumTestTransport = getDebeziumTestTransport();
        Configuration.Builder configBuilder = TestHelper.defaultConfig(
                TestHelper.TEST_DATABASE_1,
                TestHelper.TEST_DATABASE_2)
                .with("tasks.max", 2)
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SqlServerConnectorConfig.SnapshotMode.INITIAL.getValue())
                .with("openlineage.integration.enabled", true)
                .with("openlineage.integration.config.file.path", getClass().getClassLoader().getResource("openlineage/openlineage.yml").getPath())
                .with("openlineage.integration.job.description", "This connector does cdc for products")
                .with("openlineage.integration.job.tags", "env=prod,team=cdc")
                .with("openlineage.integration.job.owners", "Mario=maintainer,John Doe=Data scientist");

        start(SqlServerConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        TestHelper.waitForDatabaseSnapshotsToBeCompletedWithMultipleTasks(TestHelper.TEST_DATABASE_1, TestHelper.TEST_DATABASE_2);

        List<OpenLineage.RunEvent> runEvents = debeziumTestTransport.getRunEvents().stream()
                .filter(runningEventsWithOutInputDatasets())
                .sorted(Comparator.comparing(event -> event.getJob().getName()))
                .toList();

        assertThat(runEvents).hasSize(2);

        assertEventContainsExpectedData(runEvents.get(0), "0", "testDB1");
        assertEventContainsExpectedData(runEvents.get(1), "1", "testDB2");

    }

    private static @NotNull Predicate<OpenLineage.RunEvent> runningEventsWithOutInputDatasets() {
        return e -> e.getEventType() == OpenLineage.RunEvent.EventType.RUNNING &&
                e.getInputs().isEmpty();
    }

    private static DebeziumTestTransport getDebeziumTestTransport() {
        ServiceLoader<TransportBuilder> loader = ServiceLoader.load(TransportBuilder.class);
        Optional<TransportBuilder> optionalBuilder = StreamSupport.stream(loader.spliterator(), false)
                .filter(b -> b.getType().equals("debezium"))
                .findFirst();

        return (DebeziumTestTransport) optionalBuilder.orElseThrow(
                () -> new IllegalArgumentException("Failed to find TransportBuilder")).build(null);
    }

    private static void assertEventContainsExpectedData(OpenLineage.RunEvent startEvent, String taskId, String databaseName) {

        assertThat(startEvent.getJob().getNamespace()).isEqualTo("server1");
        assertThat(startEvent.getJob().getName()).isEqualTo("server1." + taskId);
        assertThat(startEvent.getJob().getFacets().getDocumentation().getDescription()).isEqualTo("This connector does cdc for products");

        assertThat(startEvent.getRun().getFacets().getProcessing_engine().getName()).isEqualTo("Debezium");
        assertThat(startEvent.getRun().getFacets().getProcessing_engine().getVersion()).matches("^\\d+\\.\\d+\\.\\d+(\\.Final|-SNAPSHOT)$");
        assertThat(startEvent.getRun().getFacets().getProcessing_engine().getOpenlineageAdapterVersion()).matches("^\\d+\\.\\d+\\.\\d+$");

        DebeziumConfigFacet debeziumConfigFacet = (DebeziumConfigFacet) startEvent.getRun().getFacets().getAdditionalProperties().get("debezium_config");

        assertThat(debeziumConfigFacet.getConfigs()).contains(
                "connector.class=io.debezium.connector.sqlserver.SqlServerConnector",
                "database.names=" + databaseName,
                "database.hostname=localhost",
                "database.password=Password!",
                "database.port=1433",
                "database.user=sa",
                "errors.max.retries=-1",
                "errors.retry.delay.initial.ms=300",
                "errors.retry.delay.max.ms=10000",
                "key.converter=org.apache.kafka.connect.json.JsonConverter",
                "name=testing-connector",
                "tasks.max=2",
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
                "snapshot.mode=initial",
                "topic.prefix=server1",
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
