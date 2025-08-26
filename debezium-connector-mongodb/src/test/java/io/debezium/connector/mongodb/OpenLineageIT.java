/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static io.debezium.connector.mongodb.MongoDbConnectorConfig.FiltersMatchMode.LITERAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertOneOptions;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.data.Envelope;
import io.debezium.data.Envelope.Operation;
import io.debezium.openlineage.DebeziumTestTransport;
import io.debezium.openlineage.facets.DebeziumConfigFacet;
import io.debezium.util.Testing;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.transports.TransportBuilder;

public class OpenLineageIT extends AbstractMongoConnectorIT {

    @Before
    public void beforeEach() {
        Debug.disable();
        Print.disable();
        stopConnector();
        initializeConnectorTestFramework();
        getDebeziumTestTransport().clear();
    }

    @Test
    public void shouldProduceOpenLineageStartEvent() throws InterruptedException {

        DebeziumTestTransport debeziumTestTransport = getDebeziumTestTransport();

        // Use the DB configuration to define the connector's configuration ...
        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MongoDbConnectorConfig.DATABASE_INCLUDE_LIST, "inc")
                .with(MongoDbConnectorConfig.FILTERS_MATCH_MODE, LITERAL)
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .with("openlineage.integration.enabled", true)
                .with("openlineage.integration.config.file.path", getClass().getClassLoader().getResource("openlineage/openlineage.yml").getPath())
                .with("openlineage.integration.job.description", "This connector does cdc for products")
                .with("openlineage.integration.job.tags", "env=prod,team=cdc")
                .with("openlineage.integration.job.owners", "Mario=maintainer,John Doe=Data scientist")
                .build();

        // Set up the replication context for connections ...
        context = new MongoDbTaskContext(config);

        // Cleanup database
        TestHelper.cleanDatabase(mongo, "inc");
        TestHelper.cleanDatabase(mongo, "exc");

        // Before starting the connector, add data to the databases ...
        storeDocuments("inc", "simpletons", "simple_objects.json");
        storeDocuments("exc", "restaurants", "restaurants1.json");

        // Start the connector ...
        start(MongoDbConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        SourceRecords records = consumeRecordsByTopic(6);

        assertThat(records.recordsForTopic("mongo.inc.simpletons").size()).isEqualTo(6);
        assertThat(records.recordsForTopic("mongo.exc.restaurants")).isNull();

        // At this point, the connector has performed the initial sync and awaits changes ...

        Optional<OpenLineage.RunEvent> runEvent = debeziumTestTransport.getRunEvents().stream()
                .filter(e -> e.getEventType() == OpenLineage.RunEvent.EventType.START)
                .findFirst();

        assertThat(runEvent).isPresent();

        assertEventContainsExpectedData(runEvent.get());

        // ---------------------------------------------------------------------------------------------------------------
        // Stop the connector
        // ---------------------------------------------------------------------------------------------------------------
        stopConnector();
    }

    @Test
    public void shouldProduceOpenLineageInputDataset() throws Exception {

        DebeziumTestTransport debeziumTestTransport = getDebeziumTestTransport();

        // Use the DB configuration to define the connector's configuration ...
        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MongoDbConnectorConfig.DATABASE_INCLUDE_LIST, "inc")
                .with(MongoDbConnectorConfig.FILTERS_MATCH_MODE, LITERAL)
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .with("openlineage.integration.enabled", true)
                .with("openlineage.integration.config.file.path", getClass().getClassLoader().getResource("openlineage/openlineage.yml").getPath())
                .with("openlineage.integration.job.description", "This connector does cdc for products")
                .with("openlineage.integration.job.tags", "env=prod,team=cdc")
                .with("openlineage.integration.job.owners", "Mario=maintainer,John Doe=Data scientist")
                .build();

        // Set up the replication context for connections ...
        context = new MongoDbTaskContext(config);

        // Cleanup database
        TestHelper.cleanDatabase(mongo, "inc");
        TestHelper.cleanDatabase(mongo, "exc");

        // Before starting the connector, add data to the databases ...
        storeDocuments("inc", "simpletons", "simple_objects.json");
        storeDocuments("exc", "restaurants", "restaurants1.json");

        // Start the connector ...
        start(MongoDbConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        SourceRecords records = consumeRecordsByTopic(6);

        assertThat(records.recordsForTopic("mongo.inc.simpletons").size()).isEqualTo(6);
        assertThat(records.recordsForTopic("mongo.exc.restaurants")).isNull();

        // At this point, the connector has performed the initial sync and awaits changes ...

        List<OpenLineage.RunEvent> runningEvents = debeziumTestTransport.getRunEvents().stream()
                .filter(e -> e.getEventType() == OpenLineage.RunEvent.EventType.RUNNING)
                .toList();

        assertThat(runningEvents).hasSize(2);

        assertEventContainsExpectedData(runningEvents.get(0));
        assertEventContainsExpectedData(runningEvents.get(1));

        assertCorrectInputDataset(runningEvents.get(1).getInputs(), "inc.simpletons", List.of());

    }

    @Test
    public void shouldProduceOpenLineageInputDatasetEvenWhenNoSnapstho() throws Exception {

        DebeziumTestTransport debeziumTestTransport = getDebeziumTestTransport();

        // Use the DB configuration to define the connector's configuration ...
        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MongoDbConnectorConfig.DATABASE_INCLUDE_LIST, "inc")
                .with(MongoDbConnectorConfig.FILTERS_MATCH_MODE, LITERAL)
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .with(MongoDbConnectorConfig.SNAPSHOT_MODE, "no_data")
                .with("openlineage.integration.enabled", true)
                .with("openlineage.integration.config.file.path", getClass().getClassLoader().getResource("openlineage/openlineage.yml").getPath())
                .with("openlineage.integration.job.description", "This connector does cdc for products")
                .with("openlineage.integration.job.tags", "env=prod,team=cdc")
                .with("openlineage.integration.job.owners", "Mario=maintainer,John Doe=Data scientist")
                .build();

        // Set up the replication context for connections ...
        context = new MongoDbTaskContext(config);

        // Cleanup database
        TestHelper.cleanDatabase(mongo, "inc");
        TestHelper.cleanDatabase(mongo, "exc");

        // Start the connector ...
        start(MongoDbConnector.class, config);

        waitForStreamingRunning("mongodb", "mongo");

        // Before starting the connector, add data to the databases ...
        storeDocuments("inc", "simpletons", "simple_objects.json");
        storeDocuments("exc", "restaurants", "restaurants1.json");

        SourceRecords records = consumeRecordsByTopic(6);

        assertThat(records.recordsForTopic("mongo.inc.simpletons").size()).isEqualTo(6);
        assertThat(records.recordsForTopic("mongo.exc.restaurants")).isNull();

        List<OpenLineage.RunEvent> runningEvents = debeziumTestTransport.getRunEvents().stream()
                .filter(e -> e.getEventType() == OpenLineage.RunEvent.EventType.RUNNING)
                .toList();

        assertThat(runningEvents).hasSize(2);

        assertEventContainsExpectedData(runningEvents.get(0));
        assertEventContainsExpectedData(runningEvents.get(1));

        assertCorrectInputDataset(runningEvents.get(1).getInputs(), "inc.simpletons", List.of());

    }

    private static void assertCorrectInputDataset(List<OpenLineage.InputDataset> inputs, String expectedTableName, List<String> expectedFields) {
        assertThat(inputs).hasSize(1);
        assertThat(inputs.get(0).getName()).isEqualTo(expectedTableName);
        assertThat(inputs.get(0).getNamespace()).matches("mongodb://[\\d.]+:\\d+");
    }

    private static void assertEventContainsExpectedData(OpenLineage.RunEvent startEvent) {

        assertThat(startEvent.getJob().getNamespace()).isEqualTo("mongo");
        assertThat(startEvent.getJob().getName()).isEqualTo("mongo.0");
        assertThat(startEvent.getJob().getFacets().getDocumentation().getDescription()).isEqualTo("This connector does cdc for products");

        assertThat(startEvent.getRun().getFacets().getProcessing_engine().getName()).isEqualTo("Debezium");
        assertThat(startEvent.getRun().getFacets().getProcessing_engine().getVersion()).matches("^\\d+\\.\\d+\\.\\d+.*$");
        assertThat(startEvent.getRun().getFacets().getProcessing_engine().getOpenlineageAdapterVersion()).matches("^\\d+\\.\\d+\\.\\d+$");

        DebeziumConfigFacet debeziumConfigFacet = (DebeziumConfigFacet) startEvent.getRun().getFacets().getAdditionalProperties().get("debezium_config");

        assertThat(debeziumConfigFacet.getConfigs()).contains(
                "connector.class=io.debezium.connector.mongodb.MongoDbConnector",
                "database.include.list=inc",
                "filters.match.mode=literal",
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
                "topic.prefix=mongo",
                "value.converter=org.apache.kafka.connect.json.JsonConverter")
                .anyMatch(config -> config.startsWith("mongodb.connection.string=") && config.contains("mongodb://"))
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

    private static DebeziumTestTransport getDebeziumTestTransport() {
        ServiceLoader<TransportBuilder> loader = ServiceLoader.load(TransportBuilder.class);
        Optional<TransportBuilder> optionalBuilder = StreamSupport.stream(loader.spliterator(), false)
                .filter(b -> b.getType().equals("debezium"))
                .findFirst();

        return (DebeziumTestTransport) optionalBuilder.orElseThrow(
                () -> new IllegalArgumentException("Failed to find TransportBuilder")).build(null);
    }

    protected void verifyFromInitialSnapshot(SourceRecord record, AtomicBoolean foundLast) {
        if (record.sourceOffset().containsKey(SourceInfo.INITIAL_SYNC)) {
            assertThat(record.sourceOffset().containsKey(SourceInfo.INITIAL_SYNC)).isTrue();
            Struct value = (Struct) record.value();
            assertThat(value.getStruct(Envelope.FieldName.SOURCE).getString(SourceInfo.SNAPSHOT_KEY)).isEqualTo("true");
        }
        else {
            // Only the last record in the initial sync should be marked as not being part of the initial sync ...
            assertThat(foundLast.getAndSet(true)).isFalse();
            Struct value = (Struct) record.value();
            assertThat(value.getStruct(Envelope.FieldName.SOURCE).getString(SourceInfo.SNAPSHOT_KEY)).isEqualTo("last");
        }
    }

    protected void verifyNotFromInitialSnapshot(SourceRecord record) {
        assertThat(record.sourceOffset().containsKey(SourceInfo.INITIAL_SYNC)).isFalse();
        Struct value = (Struct) record.value();
        assertThat(value.getStruct(Envelope.FieldName.SOURCE).getString(SourceInfo.SNAPSHOT_KEY)).isNull();
    }

    protected void verifyCreateOperation(SourceRecord record) {
        verifyOperation(record, Operation.CREATE);
    }

    protected void verifyReadOperation(SourceRecord record) {
        verifyOperation(record, Operation.READ);
    }

    protected void verifyUpdateOperation(SourceRecord record) {
        verifyOperation(record, Operation.UPDATE);
    }

    protected void verifyDeleteOperation(SourceRecord record) {
        verifyOperation(record, Operation.DELETE);
    }

    protected void verifyOperation(SourceRecord record, Operation expected) {
        Struct value = (Struct) record.value();
        assertThat(value.getString(Envelope.FieldName.OPERATION)).isEqualTo(expected.code());
    }

    protected void storeDocuments(String dbName, String collectionName, String pathOnClasspath) {
        try (var client = connect()) {
            Testing.debug("Storing in '" + dbName + "." + collectionName + "' documents loaded from from '" + pathOnClasspath + "'");
            MongoDatabase db1 = client.getDatabase(dbName);
            MongoCollection<Document> coll = db1.getCollection(collectionName);
            coll.drop();
            storeDocuments(coll, pathOnClasspath);
        }
    }

    protected void storeDocuments(MongoCollection<Document> collection, String pathOnClasspath) {
        InsertOneOptions insertOptions = new InsertOneOptions().bypassDocumentValidation(true);
        loadTestDocuments(pathOnClasspath).forEach(doc -> {
            assertThat(doc).isNotNull();
            assertThat(doc.size()).isGreaterThan(0);
            collection.insertOne(doc, insertOptions);
        });
    }

}
