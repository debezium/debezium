/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.heartbeat;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Map;

import jakarta.enterprise.event.Event;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.debezium.connector.SnapshotRecord;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.runtime.Connector;
import io.debezium.runtime.Debezium;
import io.debezium.runtime.DebeziumStatus;
import io.debezium.runtime.events.DebeziumHeartbeat;
import io.debezium.spi.schema.DataCollectionId;

class QuarkusHeartbeatEmitterTest {

    public static final DebeziumStatus DEBEZIUM_STATUS = new DebeziumStatus(DebeziumStatus.State.POLLING);
    public static final Connector CONNECTOR = new Connector("test.connector");
    public static final Map<String, String> PARTITION = Map.of("key", "value");

    private final Debezium debezium = Mockito.mock(Debezium.class);
    private final Event event = Mockito.mock(Event.class);
    private final QuarkusHeartbeatEmitter underTest = new QuarkusHeartbeatEmitter(debezium, event);

    @Test
    @DisplayName("should fire an event when called")
    void shouldFireEventWhenCalled() {
        when(debezium.status()).thenReturn(DEBEZIUM_STATUS);
        when(debezium.connector()).thenReturn(CONNECTOR);

        underTest.emit(PARTITION, OFFSET);

        verify(event).fire(new DebeziumHeartbeat(CONNECTOR, DEBEZIUM_STATUS, PARTITION, Map.of("offset", "value")));
    }

    public static final OffsetContext OFFSET = new OffsetContext() {
        @Override
        public Map<String, ?> getOffset() {
            return Map.of("offset", "value");
        }

        @Override
        public Schema getSourceInfoSchema() {
            return null;
        }

        @Override
        public Struct getSourceInfo() {
            return null;
        }

        @Override
        public boolean isInitialSnapshotRunning() {
            return false;
        }

        @Override
        public void markSnapshotRecord(SnapshotRecord record) {

        }

        @Override
        public void preSnapshotStart(boolean onDemand) {

        }

        @Override
        public void preSnapshotCompletion() {

        }

        @Override
        public void postSnapshotCompletion() {

        }

        @Override
        public void event(DataCollectionId collectionId, Instant timestamp) {

        }

        @Override
        public TransactionContext getTransactionContext() {
            return null;
        }
    };
}
