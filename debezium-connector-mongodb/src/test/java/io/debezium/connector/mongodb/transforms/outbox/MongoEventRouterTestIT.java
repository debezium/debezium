/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms.outbox;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.AbstractMongoConnectorIT;
import io.debezium.connector.mongodb.MongoDbConnector;
import io.debezium.connector.mongodb.MongoDbConnectorConfig;
import io.debezium.connector.mongodb.MongoDbTaskContext;
import io.debezium.connector.mongodb.TestHelper;

/**
 * Integration tests for {@link MongoEventRouter}
 *
 * @author Sungho Hwang
 */
public class MongoEventRouterTestIT extends AbstractMongoConnectorIT {
    protected static final String DB_NAME = "db";
    protected static final String SERVER_NAME = "mongo";

    private MongoEventRouter<SourceRecord> outboxEventRouter;

    @Before
    public void beforeEach() {
        // Use the DB configuration to define the connector's configuration ...
        Configuration config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, DB_NAME + "." + this.getCollectionName())
                .with(CommonConnectorConfig.TOPIC_PREFIX, SERVER_NAME)
                .build();

        beforeEach(config);
    }

    private String getCollectionName() {
        return "test";
    }

    protected String topicName() {
        return String.format("%s.%s.%s", SERVER_NAME, DB_NAME, this.getCollectionName());
    }

    public void beforeEach(Configuration config) {
        Debug.disable();
        Print.disable();
        stopConnector();
        initializeConnectorTestFramework();

        outboxEventRouter = new MongoEventRouter<>();
        outboxEventRouter.configure(Collections.emptyMap());

        // Set up the replication context for connections ...
        context = new MongoDbTaskContext(config);

        // Cleanup database
        TestHelper.cleanDatabase(mongo, DB_NAME);

        // Start the connector ...
        start(MongoDbConnector.class, config);
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            if (context != null) {
                context.getConnectionContext().close();
            }
        }
        outboxEventRouter.close();
    }

    @Test
    public void shouldConsumeRecordsFromInsert() throws Exception {
        try (var client = connect()) {
            client.getDatabase(DB_NAME).getCollection(this.getCollectionName())
                    .insertOne(new Document()
                            .append("aggregateid", 123L)
                            .append("aggregatetype", "Order")
                            .append("type", "OrderCreated")
                            .append("payload", new Document()
                                    .append("_id", new ObjectId("000000000000000000000000"))
                                    .append("fullName", "John Doe")
                                    .append("enabled", true)
                                    .append("asset", 100000L)
                                    .append("age", 42)
                                    .append("pets", Arrays.asList("dog", "cat"))));
        }

        SourceRecords actualRecords = consumeRecordsByTopic(1);
        assertThat(actualRecords.topics().size()).isEqualTo(1);

        SourceRecord newEventRecord = actualRecords.recordsForTopic(topicName()).get(0);
        SourceRecord routedEvent = outboxEventRouter.apply(newEventRecord);

        assertThat(routedEvent).isNotNull();
        assertThat(routedEvent.topic()).isEqualTo("outbox.event.Order");

        assertThat(routedEvent.keySchema().type()).isEqualTo(Schema.Type.INT64);
        assertThat(routedEvent.key()).isEqualTo(123L);

        Object value = routedEvent.value();
        assertThat(value).isInstanceOf(String.class);
        Document payload = Document.parse((String) value);
        assertThat(payload.get("_id")).isEqualTo(new ObjectId("000000000000000000000000"));
        assertThat(payload.get("fullName")).isEqualTo("John Doe");
        assertThat(payload.get("asset")).isEqualTo(100000L);
        assertThat(payload.get("enabled")).isEqualTo(true);
        assertThat(payload.get("pets")).isEqualTo(Arrays.asList("dog", "cat"));
    }

    @Test
    public void shouldSendEventTypeAsHeader() throws Exception {
        try (var client = connect()) {
            client.getDatabase(DB_NAME).getCollection(this.getCollectionName())
                    .insertOne(new Document()
                            .append("aggregateid", 123L)
                            .append("aggregatetype", "Order")
                            .append("type", "OrderCreated")
                            .append("payload", new Document()
                                    .append("_id", new ObjectId("000000000000000000000000"))
                                    .append("fullName", "John Doe")
                                    .append("asset", 100000L)));
        }

        final Map<String, String> config = new HashMap<>();
        config.put(
                MongoEventRouterConfigDefinition.FIELDS_ADDITIONAL_PLACEMENT.name(),
                "type:header:eventType");
        outboxEventRouter.configure(config);

        SourceRecords actualRecords = consumeRecordsByTopic(1);
        assertThat(actualRecords.topics().size()).isEqualTo(1);

        SourceRecord newEventRecord = actualRecords.recordsForTopic(topicName()).get(0);
        SourceRecord routedEvent = outboxEventRouter.apply(newEventRecord);

        assertThat(routedEvent).isNotNull();
        assertThat(routedEvent.topic()).isEqualTo("outbox.event.Order");

        Object value = routedEvent.value();
        assertThat(routedEvent.headers().lastWithName("eventType").value()).isEqualTo("OrderCreated");
        assertThat(routedEvent.key()).isEqualTo(123L);
        assertThat(value).isInstanceOf(String.class);

        Document payload = Document.parse((String) value);
        assertThat(payload.get("_id")).isEqualTo(new ObjectId("000000000000000000000000"));
        assertThat(payload.get("fullName")).isEqualTo("John Doe");
        assertThat(payload.get("asset")).isEqualTo(100000L);
    }

    @Test
    public void shouldSendEventTypeAsValue() throws Exception {
        try (var client = connect()) {
            client.getDatabase(DB_NAME).getCollection(this.getCollectionName())
                    .insertOne(new Document()
                            .append("aggregateid", 123L)
                            .append("aggregatetype", "Order")
                            .append("type", "OrderCreated")
                            .append("payload", new Document()
                                    .append("_id", new ObjectId("000000000000000000000000"))
                                    .append("fullName", "John Doe")
                                    .append("asset", 100000L)
                                    .append("age", 42)));
        }

        final Map<String, String> config = new HashMap<>();
        config.put(
                MongoEventRouterConfigDefinition.FIELDS_ADDITIONAL_PLACEMENT.name(),
                "type:envelope:eventType");
        outboxEventRouter.configure(config);

        SourceRecords actualRecords = consumeRecordsByTopic(1);
        assertThat(actualRecords.topics().size()).isEqualTo(1);

        SourceRecord newEventRecord = actualRecords.recordsForTopic(topicName()).get(0);
        SourceRecord routedEvent = outboxEventRouter.apply(newEventRecord);

        assertThat(routedEvent).isNotNull();
        assertThat(routedEvent.topic()).isEqualTo("outbox.event.Order");

        Struct valueStruct = requireStruct(routedEvent.value(), "test payload");
        assertThat(valueStruct.getString("eventType")).isEqualTo("OrderCreated");
        Document payload = Document.parse((String) valueStruct.get("payload"));
        assertThat(payload.get("_id")).isEqualTo(new ObjectId("000000000000000000000000"));
        assertThat(payload.get("fullName")).isEqualTo("John Doe");
        assertThat(payload.get("asset")).isEqualTo(100000L);
    }

    @Test
    public void shouldSupportAllFeatures() throws Exception {
        try (var client = connect()) {
            client.getDatabase(DB_NAME).getCollection(this.getCollectionName())
                    .insertOne(new Document()
                            .append("_id", new ObjectId("111111111111111111111111"))
                            .append("aggregateid", 123L)
                            .append("aggregatetype", "Order")
                            .append("type", "OrderCreated")
                            .append("somebooltype", true)
                            .append("version", 2)
                            .append("createdat", 12342452512L)
                            .append("is_deleted", false)
                            .append("payload", new Document()
                                    .append("_id", new ObjectId("000000000000000000000000"))
                                    .append("fullName", "John Doe")
                                    .append("asset", 100000L)
                                    .append("age", 42)));
        }

        final Map<String, String> config = new HashMap<>();
        config.put(MongoEventRouterConfigDefinition.FIELD_SCHEMA_VERSION.name(), "version");
        config.put(MongoEventRouterConfigDefinition.FIELD_EVENT_TIMESTAMP.name(), "createdat");
        config.put(
                MongoEventRouterConfigDefinition.FIELDS_ADDITIONAL_PLACEMENT.name(),
                "version:envelope:eventVersion," +
                        "aggregatetype:envelope:aggregateType," +
                        "somebooltype:envelope:someBoolType," +
                        "somebooltype:header," +
                        "is_deleted:envelope:deleted");
        outboxEventRouter.configure(config);

        SourceRecords actualRecords = consumeRecordsByTopic(1);
        assertThat(actualRecords.topics().size()).isEqualTo(1);

        SourceRecord newEventRecord = actualRecords.recordsForTopic(topicName()).get(0);
        SourceRecord eventRouted = outboxEventRouter.apply(newEventRecord);

        assertThat(eventRouted.timestamp()).isEqualTo(12342452512L);
        assertThat(eventRouted.topic()).isEqualTo("outbox.event.Order");

        // Validate headers
        Headers headers = eventRouted.headers();
        assertThat(headers.size()).isEqualTo(2);
        Header headerId = headers.lastWithName("id");
        assertThat(headerId.schema()).isEqualTo(Schema.OPTIONAL_STRING_SCHEMA);
        assertThat(headerId.value()).isEqualTo("111111111111111111111111");
        Header headerBool = headers.lastWithName("somebooltype");
        assertThat(headerBool.schema()).isEqualTo(Schema.OPTIONAL_BOOLEAN_SCHEMA);
        assertThat(headerBool.value()).isEqualTo(true);

        // Validate Key
        assertThat(eventRouted.keySchema()).isEqualTo(SchemaBuilder.OPTIONAL_INT64_SCHEMA);
        assertThat(eventRouted.key()).isEqualTo(123L);

        // Validate message body
        Struct valueStruct = requireStruct(eventRouted.value(), "test envelope");
        assertThat(valueStruct.getString("aggregateType")).isEqualTo("Order");
        assertThat(valueStruct.getInt32("eventVersion")).isEqualTo(2);
        assertThat(valueStruct.getBoolean("someBoolType")).isEqualTo(true);
        assertThat(valueStruct.getBoolean("deleted")).isEqualTo(false);
    }

    @Test
    public void shouldNotProduceTombstoneEventForNullPayload() throws Exception {
        try (var client = connect()) {
            client.getDatabase(DB_NAME).getCollection(this.getCollectionName())
                    .insertOne(new Document()
                            .append("_id", new ObjectId("000000000000000000000000"))
                            .append("aggregateid", 123L)
                            .append("aggregatetype", "Order")
                            .append("type", "OrderCreated")
                            .append("payload", null));
        }

        final Map<String, String> config = new HashMap<>();
        config.put(
                MongoEventRouterConfigDefinition.FIELDS_ADDITIONAL_PLACEMENT.name(),
                "aggregatetype:envelope:aggregateType");
        outboxEventRouter.configure(config);

        SourceRecords actualRecords = consumeRecordsByTopic(1);
        assertThat(actualRecords.topics().size()).isEqualTo(1);

        SourceRecord newEventRecord = actualRecords.recordsForTopic(topicName()).get(0);
        SourceRecord eventRouted = outboxEventRouter.apply(newEventRecord);

        // Validate metadata
        assertThat(eventRouted.topic()).isEqualTo("outbox.event.Order");

        // Validate headers
        Headers headers = eventRouted.headers();
        assertThat(headers.size()).isEqualTo(1);
        Header headerId = headers.lastWithName("id");
        assertThat(headerId.value()).isEqualTo("000000000000000000000000");

        // Validate Key
        assertThat(eventRouted.keySchema().type()).isEqualTo(Schema.Type.INT64);
        assertThat(eventRouted.key()).isEqualTo(123L);

        // Validate message body
        assertThat(eventRouted.valueSchema()).isNotNull();
        assertThat(eventRouted.value()).isNotNull();
        assertThat(((Struct) eventRouted.value()).get("payload")).isNull();
    }

    @Test
    public void shouldProduceTombstoneEventForNullPayload() throws Exception {
        try (var client = connect()) {
            client.getDatabase(DB_NAME).getCollection(this.getCollectionName())
                    .insertOne(new Document()
                            .append("_id", new ObjectId("000000000000000000000000"))
                            .append("aggregateid", 123L)
                            .append("aggregatetype", "Order")
                            .append("type", "OrderCreated")
                            .append("payload", null));
        }

        final Map<String, String> config = new HashMap<>();
        config.put("route.tombstone.on.empty.payload", "true");
        outboxEventRouter.configure(config);

        SourceRecords actualRecords = consumeRecordsByTopic(1);
        assertThat(actualRecords.topics().size()).isEqualTo(1);

        SourceRecord newEventRecord = actualRecords.recordsForTopic(topicName()).get(0);
        SourceRecord eventRouted = outboxEventRouter.apply(newEventRecord);

        // Validate metadata
        assertThat(eventRouted.topic()).isEqualTo("outbox.event.Order");

        // Validate headers
        Headers headers = eventRouted.headers();
        assertThat(headers.size()).isEqualTo(1);
        Header headerId = headers.lastWithName("id");
        assertThat(headerId.value()).isEqualTo("000000000000000000000000");

        // Validate Key
        assertThat(eventRouted.keySchema().type()).isEqualTo(Schema.Type.INT64);
        assertThat(eventRouted.key()).isEqualTo(123L);

        // Validate message body
        assertThat(eventRouted.valueSchema()).isNull();
        assertThat(eventRouted.value()).isNull();
    }
}
