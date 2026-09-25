/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.nats.history;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.relational.Tables;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.SchemaHistoryException;
import io.debezium.relational.history.SchemaHistoryListener;
import io.debezium.storage.nats.NatsCommonConfig;
import io.debezium.util.Collect;
import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Subscription;
import io.nats.client.support.NatsJetStreamConstants;

/**
 * Tests that a schema history record is neither silently lost nor silently stored
 * twice, since either leaves the connector describing the wrong table structure.
 *
 * @author Nick Chomey
 */
@Testcontainers
class NatsSchemaHistoryIntegrityIT {

    private static final String NATS_CONTAINER_IMAGE = "nats:2.12.0-alpine";
    private static final int NATS_PORT = 4222;
    private static final String STREAM_NAME = "integrity-schema-history";
    private static final String SUBJECT = "integrity.schema.history";

    @Container
    @SuppressWarnings("resource")
    public GenericContainer<?> natsContainer = new GenericContainer<>(DockerImageName.parse(NATS_CONTAINER_IMAGE))
            .withExposedPorts(NATS_PORT)
            .withCommand("-js");

    private String natsUrl;
    private SchemaHistory history;

    @BeforeEach
    public void setUp() {
        natsUrl = "nats://localhost:" + natsContainer.getMappedPort(NATS_PORT);
        history = createHistory(SchemaHistoryListener.NOOP);
    }

    @AfterEach
    public void tearDown() {
        if (history != null) {
            history.stop();
        }
    }

    @Test
    @Timeout(30)
    @SuppressWarnings("deprecation")
    public void shouldFailRecoveryWhenARecordCannotBeHandled() throws Exception {
        // A record that is logged and skipped leaves recovery to finish with a partial
        // schema model, so the connector emits events for the wrong table structure. A
        // failure while handling a recovered record therefore has to fail recovery.
        Map<String, Object> source = server();
        history.record(source, position(1), "testdb", "CREATE TABLE t1 (id INT);");

        SchemaHistory failing = createHistory(failsOnRecovery());
        try {
            Tables tables = new Tables();
            assertThrows(SchemaHistoryException.class,
                    () -> failing.recover(source, position(1), tables, new MySqlAntlrDdlParser()));
        }
        finally {
            failing.stop();
        }
    }

    @Test
    @Timeout(30)
    @SuppressWarnings("deprecation")
    public void shouldTagAPublishWithAMessageId() throws Exception {
        // JetStream can only recognize a retried publish as a duplicate when the message
        // carries an ID, so every schema history record has to be published with one.
        try (Connection conn = Nats.connect(natsUrl)) {
            Subscription subscription = conn.subscribe(SUBJECT);
            try {
                history.record(server(), position(1), "testdb", "CREATE TABLE t1 (id INT);");

                Message message = subscription.nextMessage(Duration.ofSeconds(10));
                assertThat(message).as("the recorded DDL should have been published to '%s'", SUBJECT).isNotNull();
                assertThat(message.getHeaders()).as("the published record should carry headers").isNotNull();
                assertThat(message.getHeaders().get(NatsJetStreamConstants.MSG_ID_HDR))
                        .as("the published record should carry a message ID so it can be deduplicated")
                        .isNotNull();
            }
            finally {
                subscription.unsubscribe();
            }
        }
    }

    /**
     * A listener that fails as soon as a record read from the history has to be handled,
     * which stands in for any failure while applying a recovered record.
     */
    private static SchemaHistoryListener failsOnRecovery() {
        return new SchemaHistoryListener() {
            @Override
            public void started() {
            }

            @Override
            public void stopped() {
            }

            @Override
            public void recoveryStarted() {
            }

            @Override
            public void recoveryStopped() {
            }

            @Override
            public void onChangeFromHistory(HistoryRecord record) {
                throw new IllegalStateException("Cannot handle the recovered record " + record);
            }

            @Override
            public void onChangeApplied(HistoryRecord record) {
            }
        };
    }

    @SuppressWarnings("deprecation")
    private SchemaHistory createHistory(SchemaHistoryListener listener) {
        Configuration configuration = Configuration.from(Collect.hashMapOf(
                SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsCommonConfig.NATS_URL.name(), natsUrl,
                SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsSchemaHistoryConfig.PROP_STREAM_NAME.name(),
                STREAM_NAME,
                SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsSchemaHistoryConfig.PROP_SUBJECT.name(),
                SUBJECT));

        NatsSchemaHistory configured = new NatsSchemaHistory();
        configured.configure(configuration, null, listener, true);
        configured.initializeStorage();
        configured.start();
        return configured;
    }

    private static Map<String, Object> server() {
        return Collect.linkMapOf("server", "integrity-server");
    }

    private static Map<String, Object> position(int entry) {
        return Collect.linkMapOf("file", "integrity.log", "position", 1, "entry", entry);
    }
}
