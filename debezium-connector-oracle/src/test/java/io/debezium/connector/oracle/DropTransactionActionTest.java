/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.logminer.buffered.BufferedLogMinerStreamingChangeEventSource;
import io.debezium.connector.oracle.spi.DropTransactionAction;
import io.debezium.doc.FixFor;
import io.debezium.document.Document;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.signal.SignalPayload;

/**
 * Unit tests for {@link DropTransactionAction}.
 *
 * @author Nathan Smit
 */
@SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER_BUFFERED)
public class DropTransactionActionTest {

    @Rule
    public final TestRule skipAdapterRule = new SkipTestDependingOnAdapterNameRule();

    private DropTransactionAction<OraclePartition> action;
    private ChangeEventSourceCoordinator<OraclePartition, OracleOffsetContext> coordinator;
    private BufferedLogMinerStreamingChangeEventSource streamingSource;
    private OraclePartition partition;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        coordinator = mock(ChangeEventSourceCoordinator.class);
        streamingSource = mock(BufferedLogMinerStreamingChangeEventSource.class);
        partition = new OraclePartition(null, null);

        // Create the action with the coordinator
        action = new DropTransactionAction<>(coordinator);

        // Stub the coordinator to return the mocked streaming source via the new accessor
        when(coordinator.getStreamingSource()).thenReturn(Optional.of(streamingSource));
    }

    @Test
    @FixFor("DBZ-9552")
    public void shouldDropTransactionWhenTransactionExists() throws Exception {
        // Given a transaction ID
        String transactionId = "test.transaction.123";
        Document data = Document.create();
        data.setString("transaction-id", transactionId);

        SignalPayload<OraclePartition> payload = new SignalPayload<>(
                partition, "signal-id", "drop-transaction", data, null, Collections.emptyMap());

        // Mock the abandonTransactionById to return true (transaction found and dropped)
        when(streamingSource.abandonTransactionById(transactionId)).thenReturn(true);

        // When we execute the action
        boolean result = action.arrived(payload);

        // Then the transaction should be abandoned
        verify(streamingSource).abandonTransactionById(transactionId);
        assertThat(result).isTrue();
    }

    @Test
    @FixFor("DBZ-9552")
    public void shouldReturnFalseWhenTransactionNotFound() throws Exception {
        // Given a transaction ID that doesn't exist
        String transactionId = "nonexistent.transaction.456";
        Document data = Document.create();
        data.setString("transaction-id", transactionId);

        SignalPayload<OraclePartition> payload = new SignalPayload<>(
                partition, "signal-id", "drop-transaction", data, null, Collections.emptyMap());

        // Mock the abandonTransactionById to return false (transaction not found)
        when(streamingSource.abandonTransactionById(transactionId)).thenReturn(false);

        // When we execute the action
        boolean result = action.arrived(payload);

        // Then the action should return false
        verify(streamingSource).abandonTransactionById(transactionId);
        assertThat(result).isFalse();
    }

    @Test
    @FixFor("DBZ-9552")
    public void shouldThrowExceptionWhenPartitionIsNotOracle() throws Exception {
        // Given a non-Oracle partition
        String transactionId = "test.transaction.789";
        Document data = Document.create();
        data.setString("transaction-id", transactionId);

        // Create a custom test partition that is not OraclePartition
        TestPartition testPartition = new TestPartition();

        SignalPayload<TestPartition> payload = new SignalPayload<>(
                testPartition, "signal-id", "drop-transaction", data, null, Collections.emptyMap());

        // Create action for wrong partition type
        @SuppressWarnings("unchecked")
        ChangeEventSourceCoordinator<TestPartition, ?> testCoordinator = (ChangeEventSourceCoordinator<TestPartition, ?>) (Object) coordinator;
        DropTransactionAction<TestPartition> wrongAction = new DropTransactionAction<>(testCoordinator);

        // When/Then we execute the action with wrong partition type, it should throw
        assertThatThrownBy(() -> wrongAction.arrived(payload))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("is only supported by Oracle connector");
    }

    @Test
    @FixFor("DBZ-9552")
    public void shouldHandleMissingTransactionIdParameter() throws Exception {
        // Given a payload without transaction-id parameter
        Document data = Document.create();
        // Don't add transaction-id to data

        SignalPayload<OraclePartition> payload = new SignalPayload<>(
                partition, "signal-id", "drop-transaction", data, null, Collections.emptyMap());

        // When we execute the action
        boolean result = action.arrived(payload);

        // Then the action should return false
        assertThat(result).isFalse();
    }

    @Test
    @FixFor("DBZ-9552")
    public void shouldHandleNullTransactionId() throws Exception {
        // Given a payload with null transaction-id
        Document data = Document.create();
        data.set("transaction-id", null);

        SignalPayload<OraclePartition> payload = new SignalPayload<>(
                partition, "signal-id", "drop-transaction", data, null, Collections.emptyMap());

        // When we execute the action
        boolean result = action.arrived(payload);

        // Then the action should return false
        assertThat(result).isFalse();
    }

    @Test
    @FixFor("DBZ-9552")
    public void shouldHandleEmptyTransactionId() throws Exception {
        // Given a payload with empty transaction-id
        Document data = Document.create();
        data.setString("transaction-id", "");

        SignalPayload<OraclePartition> payload = new SignalPayload<>(
                partition, "signal-id", "drop-transaction", data, null, Collections.emptyMap());

        // When we execute the action
        boolean result = action.arrived(payload);

        // Then the action should return false
        assertThat(result).isFalse();
    }

    @Test
    @FixFor("DBZ-9552")
    public void shouldHandleStreamingSourceNotAvailable() throws Exception {
        // Given a payload with a valid transaction-id but no streaming source available
        String transactionId = "test.transaction.xyz";
        Document data = Document.create();
        data.setString("transaction-id", transactionId);

        SignalPayload<OraclePartition> payload = new SignalPayload<>(
                partition, "signal-id", "drop-transaction", data, null, Collections.emptyMap());

        // Mock the coordinator to return empty streaming source
        when(coordinator.getStreamingSource()).thenReturn(Optional.empty());

        // When we execute the action
        boolean result = action.arrived(payload);

        // Then the action should return false (streaming source not available)
        assertThat(result).isFalse();
    }

    // Test partition class for testing non-Oracle partition behavior
    private static class TestPartition implements io.debezium.pipeline.spi.Partition {
        @Override
        public java.util.Map<String, String> getSourcePartition() {
            return java.util.Collections.emptyMap();
        }
    }
}
