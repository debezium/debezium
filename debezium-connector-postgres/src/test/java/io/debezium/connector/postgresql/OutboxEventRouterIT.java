/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import io.debezium.config.Configuration;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.transforms.outbox.EventRouter;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.UUID;

import static io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode.INITIAL;
import static io.debezium.connector.postgresql.TestHelper.topicName;
import static org.fest.assertions.Assertions.assertThat;

/**
 * Integration test for {@link io.debezium.transforms.outbox.EventRouter} with {@link PostgresConnector}
 *
 * @author Renato Mefi (gh@mefi.in)
 */
public class OutboxEventRouterIT extends AbstractConnectorTest {

    private static final String SETUP_OUTBOX_SCHEMA = "DROP SCHEMA IF EXISTS outboxsmtit CASCADE;" +
            "CREATE SCHEMA outboxsmtit;";

    private static final String SETUP_OUTOBOX_TABLE = "CREATE TABLE outboxsmtit.outbox " +
            "(" +
            "  id            uuid         not null" +
            "    constraint outbox_pk primary key," +
            "  aggregatetype varchar(255) not null," +
            "  aggregateid   varchar(255) not null," +
            "  type          varchar(255) not null," +
            "  payload       jsonb        not null" +
            ");";

    private EventRouter<SourceRecord> outboxEventRouter;

    private static String createEventInsert(
            UUID eventId,
            String eventType,
            String aggregateType,
            String aggregateId,
            String payloadJson
    ) {
        return String.format("INSERT INTO outboxsmtit.outbox VALUES (" +
                        "'%s'" +
                        ", '%s'" +
                        ", '%s'" +
                        ", '%s'" +
                        ", '%s'::jsonb" +
                        ");",
                eventId,
                aggregateType,
                aggregateId,
                eventType,
                payloadJson);
    }

    @Before
    public void beforeEach() {
        outboxEventRouter = new EventRouter<>();
        outboxEventRouter.configure(Collections.emptyMap());

        TestHelper.execute(SETUP_OUTBOX_SCHEMA);
        TestHelper.execute(SETUP_OUTOBOX_TABLE);

        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, INITIAL.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .with(PostgresConnectorConfig.SCHEMA_WHITELIST, "outboxsmtit")
                .with(PostgresConnectorConfig.TABLE_WHITELIST, "outboxsmtit\\.outbox");
        start(PostgresConnector.class, configBuilder.build());

        assertConnectorIsRunning();
    }

    @After
    public void afterEach() {
        stopConnector();
        assertNoRecordsToConsume();
        outboxEventRouter.close();
    }

    @Test
    public void shouldConsumeRecordsFromInsert() throws Exception {
        TestHelper.execute(createEventInsert(
                UUID.fromString("59a42efd-b015-44a9-9dde-cb36d9002425"),
                "UserCreated",
                "User",
                "10711fa5",
                "{}"
        ));

        SourceRecords actualRecords = consumeRecordsByTopic(1);
        assertThat(actualRecords.topics().size()).isEqualTo(1);

        SourceRecord newEventRecord = actualRecords.recordsForTopic(topicName("outboxsmtit.outbox")).get(0);
        SourceRecord routedEvent = outboxEventRouter.apply(newEventRecord);

        assertThat(routedEvent).isNotNull();
        assertThat(routedEvent.topic()).isEqualTo("user");
    }
}
