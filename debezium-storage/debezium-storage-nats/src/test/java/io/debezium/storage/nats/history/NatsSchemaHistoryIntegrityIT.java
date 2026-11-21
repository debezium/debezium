/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.nats.history;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.document.DocumentReader;
import io.debezium.relational.Tables;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.SchemaHistoryException;
import io.debezium.relational.history.SchemaHistoryListener;
import io.debezium.storage.nats.NatsCommonConfig;
import io.debezium.storage.nats.NatsContainer;
import io.debezium.util.Collect;
import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.Nats;
import io.nats.client.api.MessageInfo;
import io.nats.client.support.NatsJetStreamConstants;

/**
 * Tests that a schema history record is neither silently lost nor silently stored
 * twice, since either leaves the connector describing the wrong table structure.
 *
 * @author Nick Chomey
 */
@Testcontainers
class NatsSchemaHistoryIntegrityIT {

    private static final String STREAM_NAME = "integrity-schema-history";
    private static final String SUBJECT = "integrity.schema.history";

    @Container
    @SuppressWarnings("resource")
    public NatsContainer natsContainer = new NatsContainer();

    private String natsUrl;
    private SchemaHistory history;

    @BeforeEach
    public void setUp() {
        natsUrl = natsContainer.getServerUrl();
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
            assertThatThrownBy(() -> failing.recover(source, position(1), tables, new MySqlAntlrDdlParser()))
                    .isInstanceOf(SchemaHistoryException.class);
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
        history.record(server(), position(1), "testdb", "CREATE TABLE t1 (id INT);");

        // The record is published synchronously, so it is already stored in the stream and
        // can be read back without depending on when a subscription becomes active.
        try (Connection conn = Nats.connect(natsUrl)) {
            MessageInfo message;
            try {
                message = conn.jetStreamManagement().getLastMessage(STREAM_NAME, SUBJECT);
            }
            catch (JetStreamApiException e) {
                if (e.getApiErrorCode() == NatsJetStreamConstants.JS_NO_MESSAGE_FOUND_ERR) {
                    throw new AssertionError(String.format(
                            "the recorded DDL should have been published to '%s', but the stream '%s' holds no message for it",
                            SUBJECT, STREAM_NAME), e);
                }
                throw e;
            }
            assertThat(message.getHeaders()).as("the published record should carry headers").isNotNull();
            assertThat(message.getHeaders().get(NatsJetStreamConstants.MSG_ID_HDR))
                    .as("the published record should carry a message ID so it can be deduplicated")
                    .isNotNull();

            // Confirms the message read back is the record this test wrote, which only matters
            // if the stream is ever shared between tests.
            HistoryRecord record = new HistoryRecord(DocumentReader.defaultReader().read(new String(message.getData(), StandardCharsets.UTF_8)));
            assertThat(record.document().getString(HistoryRecord.Fields.DDL_STATEMENTS))
                    .as("the message read back should be the record this test wrote")
                    .isEqualTo("CREATE TABLE t1 (id INT);");
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
