/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.converters;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.HeaderFrom;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.transforms.outbox.EventRouter;

/**
 * A unified test of all {@link CloudEventsConverter} behavior which all connectors should extend.
 *
 * @author Roman Kudryashov
 */
public abstract class AbstractCloudEventsConverterTest<T extends SourceConnector> extends AbstractConnectorTest {

    protected EventRouter<SourceRecord> outboxEventRouter;

    protected HeaderFrom<SourceRecord> headerFrom;

    protected abstract Class<T> getConnectorClass();

    protected abstract String getConnectorName();

    protected abstract String getServerName();

    protected abstract JdbcConnection databaseConnection();

    protected abstract Configuration.Builder getConfigurationBuilder();

    protected abstract String topicName();

    protected abstract String tableName();

    protected abstract void createTable() throws Exception;

    protected abstract String createInsert(String eventId, String eventType, String aggregateType,
                                           String aggregateId, String payloadJson, String additional);

    protected abstract void waitForStreamingStarted() throws InterruptedException;

    @Before
    public void beforeEach() throws Exception {
        createTable();

        headerFrom = new HeaderFrom.Value<>();
        Map<String, String> headerFromConfig = new LinkedHashMap<>();
        headerFromConfig.put("fields", "source,op,transaction");
        headerFromConfig.put("headers", "source,op,transaction");
        headerFromConfig.put("operation", "copy");
        headerFromConfig.put("header.converter.schemas.enable", "true");
        headerFrom.configure(headerFromConfig);

        outboxEventRouter = new EventRouter<>();
        Map<String, String> outboxEventRouterConfig = new LinkedHashMap<>();
        outboxEventRouterConfig.put("table.expand.json.payload", "true");
        outboxEventRouter.configure(outboxEventRouterConfig);

        startConnector();
    }

    @After
    public void afterEach() throws Exception {
        stopConnector();
        assertNoRecordsToConsume();
        databaseConnection().close();
        headerFrom.close();
        outboxEventRouter.close();
    }

    @Test
    @FixFor({ "DBZ-3642" })
    public void shouldConvertToCloudEventsInJsonWithMetadataInHeadersAfterOutboxEventRouter() throws Exception {
        databaseConnection().execute(createInsert(
                "59a42efd-b015-44a9-9dde-cb36d9002425",
                "UserCreated",
                "User",
                "10711fa5",
                "{" +
                        "\"someField1\": \"some value 1\"," +
                        "\"someField2\": 7005" +
                        "}",
                ""));

        SourceRecords streamingRecords = consumeRecordsByTopic(1);
        assertThat(streamingRecords.allRecordsInOrder()).hasSize(1);

        SourceRecord record = streamingRecords.recordsForTopic(topicName()).get(0);
        SourceRecord recordWithMetadataHeaders = headerFrom.apply(record);
        SourceRecord routedEvent = outboxEventRouter.apply(recordWithMetadataHeaders);

        assertThat(routedEvent).isNotNull();
        assertThat(routedEvent.topic()).isEqualTo("outbox.event.User");
        assertThat(routedEvent.keySchema()).isEqualTo(Schema.STRING_SCHEMA);
        assertThat(routedEvent.key()).isEqualTo("10711fa5");
        assertThat(routedEvent.value()).isInstanceOf(Struct.class);

        CloudEventsConverterTest.shouldConvertToCloudEventsInJsonWithMetadataInHeaders(routedEvent, getConnectorName(), getServerName());
    }

    private void startConnector() throws Exception {
        Configuration.Builder configBuilder = getConfigurationBuilder();
        start(getConnectorClass(), configBuilder.build());
        assertConnectorIsRunning();
        waitForStreamingStarted();
        assertNoRecordsToConsume();
    }
}
