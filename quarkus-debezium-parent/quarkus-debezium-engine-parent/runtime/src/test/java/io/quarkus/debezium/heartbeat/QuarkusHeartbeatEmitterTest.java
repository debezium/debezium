/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.heartbeat;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import jakarta.enterprise.event.Event;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.debezium.connector.SnapshotRecord;
import io.debezium.engine.DebeziumEngine;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.runtime.CaptureGroup;
import io.debezium.runtime.Connector;
import io.debezium.runtime.Debezium;
import io.debezium.runtime.DebeziumConnectorRegistry;
import io.debezium.runtime.DebeziumStatus;
import io.debezium.runtime.events.DebeziumHeartbeat;
import io.debezium.spi.schema.DataCollectionId;

class QuarkusHeartbeatEmitterTest {

    public static final DebeziumStatus DEBEZIUM_STATUS = new DebeziumStatus(DebeziumStatus.State.POLLING);
    public static final Connector CONNECTOR = new Connector("test.connector");
    public static final Map<String, String> PARTITION = Map.of("key", "value");

    private final DebeziumConnectorRegistry registry = Mockito.mock(DebeziumConnectorRegistry.class);
    private final Event event = Mockito.mock(Event.class);
    private final QuarkusHeartbeatEmitter underTest = new QuarkusHeartbeatEmitter(Collections.singletonList(registry), event);

    @Test
    @DisplayName("should fire an event when called")
    void shouldFireEventWhenCalled() {
        when(registry.engines()).thenReturn(Collections.singletonList(generate(DEBEZIUM_STATUS)));

        underTest.emit(PARTITION, OFFSET);

        verify(event).fire(new DebeziumHeartbeat(CONNECTOR, DEBEZIUM_STATUS, PARTITION, Map.of("offset", "value")));
    }

    @Test
    @DisplayName("should not fire an event when there are multiple engines")
    void shouldNotFireEventInMultiEngineConfiguration() {
        QuarkusHeartbeatEmitter emitter = new QuarkusHeartbeatEmitter(Collections.singletonList(registry), event);
        when(registry.engines()).thenReturn(List.of(generate(DEBEZIUM_STATUS), generate(DEBEZIUM_STATUS)));

        emitter.emit(PARTITION, OFFSET);

        verifyNoInteractions(event);
    }

    @Test
    @DisplayName("should not fire an event when there are multiple connectors")
    void shouldNotFireEventInMultiConnectorsConfiguration() {
        QuarkusHeartbeatEmitter emitter = new QuarkusHeartbeatEmitter(List.of(registry, registry), event);

        emitter.emit(PARTITION, OFFSET);

        verifyNoInteractions(registry);
        verifyNoInteractions(event);
    }

    public Debezium generate(DebeziumStatus status) {
        return new Debezium() {
            @Override
            public DebeziumEngine.Signaler signaler() {
                return null;
            }

            @Override
            public Map<String, String> configuration() {
                return Map.of();
            }

            @Override
            public DebeziumStatus status() {
                return status;
            }

            @Override
            public Connector connector() {
                return CONNECTOR;
            }

            @Override
            public CaptureGroup captureGroup() {
                return null;
            }
        };
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
