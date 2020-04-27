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
import java.util.List;
import java.util.UUID;

import static io.debezium.connector.postgresql.TestHelper.topicName;
import static org.fest.assertions.Assertions.assertThat;


/**
 * Integration test for {@link io.debezium.transforms.ByLogicalTableRouter} with {@link PostgresConnector}
 *
 * @author David Feinblum (dvfeinblum@gmail.com)
 */
public class LogicalTableRouterSMT extends AbstractConnectorTest {

    private static final String SETUP_OUTBOX_SCHEMA = "DROP SCHEMA IF EXISTS outboxsmtit CASCADE;" +
            "CREATE SCHEMA outboxsmtit;";

    private static final String SETUP_OUTOBOX_TABLE = "CREATE TABLE outboxsmtit.outbox " +
            "(" +
            "  id            uuid         not null" +
            "    constraint smt_pk primary key," +
            "  aggregatetype varchar(255) not null," +
            "  aggregateid   varchar(255) not null," +
            "  type          varchar(255) not null," +
            "  payload       jsonb" +
            ");";

    private EventRouter<SourceRecord> smtEventRouter;

    private static String createEventInsert(
            UUID eventId,
            String eventType,
            String aggregateType,
            String aggregateId,
            String payloadJson,
            String additional) {
        StringBuilder insert = new StringBuilder();
        insert.append("INSERT INTO outboxsmtit.outbox VALUES (");
        insert.append("'").append(eventId).append("'");
        insert.append(", '").append(aggregateType).append("'");
        insert.append(", '").append(aggregateId).append("'");
        insert.append(", '").append(eventType).append("'");

        if (payloadJson == null) {
            insert.append(", null::jsonb");
        }
        else if (payloadJson.isEmpty()) {
            insert.append(", ''");
        }
        else {
            insert.append(", '").append(payloadJson).append("'::jsonb");
        }

        if (additional != null) {
            insert.append(additional);
        }
        insert.append(")");

        return insert.toString();
    }

    private static String createEventUpdate(
            UUID eventId,
            String payloadJson) {

        return "UPDATE outboxsmtit.outbox SET payload = " +
                ", '" + payloadJson + "'::jsonb  WHERE id = " +
                "'" + eventId + "'";
    }

    private static String createEventDelete(
            UUID eventId) {

        return "DELETE FROM outboxsmtit.outbox WHERE id = " +
                "'" + eventId + "'";
    }

    @Before
    public void beforeEach() {
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();

        smtEventRouter = new EventRouter<>();
        smtEventRouter.configure(Collections.emptyMap());

        TestHelper.execute(SETUP_OUTBOX_SCHEMA);
        TestHelper.execute(SETUP_OUTOBOX_TABLE);
    }

    @After
    public void afterEach() {
        stopConnector();
        assertNoRecordsToConsume();
        smtEventRouter.close();
    }

    private void startConnectorWithInitialSnapshotRecord() throws Exception {
        TestHelper.execute(createEventInsert(
                UUID.fromString("70f52ae3-f671-4bac-ae62-1b9be6e73700"),
                "UserCreated",
                "User",
                "10711faf",
                "{}",
                ""));

        Configuration.Builder configBuilder = getConfigurationBuilder(PostgresConnectorConfig.SnapshotMode.INITIAL);
        start(PostgresConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        SourceRecords snapshotRecords = consumeRecordsByTopic(1);
        assertThat(snapshotRecords.allRecordsInOrder().size()).isEqualTo(1);

        List<SourceRecord> recordsFromOutbox = snapshotRecords.recordsForTopic(topicName("outboxsmtit.outbox"));
        assertThat(recordsFromOutbox.size()).isEqualTo(1);
    }

    private static Configuration.Builder getConfigurationBuilder(PostgresConnectorConfig.SnapshotMode snapshotMode) {
        return TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, snapshotMode.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .with(PostgresConnectorConfig.SCHEMA_WHITELIST, "outboxsmtit")
                .with(PostgresConnectorConfig.TABLE_WHITELIST, "outboxsmtit\\.outbox");
    }

    @Test
    public void shouldRouteDML() throws Exception {
        startConnectorWithInitialSnapshotRecord();

        final UUID initialEventId = UUID.randomUUID();

        TestHelper.execute(createEventInsert(
                initialEventId,
                "UserCreated",
                "User",
                "10711fa5",
                "{}",
                ""));

        TestHelper.execute(createEventUpdate(initialEventId, "{\"foo\": \"bar\"}"));

        TestHelper.execute(createEventDelete(initialEventId));

        // TODO: Do the SMT

        // TODO: Assert that things are changed as expected
    }

}
