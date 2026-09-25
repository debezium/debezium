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

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.logminer.buffered.BufferedLogMinerStreamingChangeEventSource;
import io.debezium.connector.oracle.logminer.buffered.PendingTransaction;
import io.debezium.connector.oracle.spi.LogPendingTransactionsAction;
import io.debezium.doc.FixFor;
import io.debezium.document.Document;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.signal.SignalPayload;
import io.debezium.pipeline.spi.Partition;

/**
 * Unit tests for {@link LogPendingTransactionsAction}.
 *
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER_BUFFERED)
public class LogPendingTransactionsActionTest {

    private LogPendingTransactionsAction<OraclePartition> action;
    private ChangeEventSourceCoordinator<OraclePartition, OracleOffsetContext> coordinator;
    private BufferedLogMinerStreamingChangeEventSource streamingSource;
    private OraclePartition partition;

    @BeforeEach
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        coordinator = mock(ChangeEventSourceCoordinator.class);
        streamingSource = mock(BufferedLogMinerStreamingChangeEventSource.class);
        partition = new OraclePartition(null, null);

        action = new LogPendingTransactionsAction<>(coordinator);

        when(coordinator.getStreamingSource()).thenReturn(Optional.of(streamingSource));
    }

    @Test
    @FixFor("debezium/dbz#2577")
    public void shouldLogActiveAndDeferredTransactions() throws Exception {
        final LogInterceptor logInterceptor = new LogInterceptor(LogPendingTransactionsAction.class);

        final PendingTransaction active = new PendingTransaction("0a.000b.0000000c", Scn.valueOf(100), Instant.parse("2024-01-01T00:00:00Z"),
                "DEBEZIUM", "client-1", 1, 5, false);
        final PendingTransaction deferred = new PendingTransaction("0d.000e.0000000f", Scn.valueOf(200), Instant.parse("2024-01-01T00:00:01Z"),
                "OTHER", null, 2, 0, true);
        when(streamingSource.getPendingTransactions()).thenReturn(List.of(active, deferred));

        final boolean result = action.arrived(createPayload(partition));

        verify(streamingSource).getPendingTransactions();
        assertThat(result).isTrue();
        assertThat(logInterceptor.containsMessage("2 total (1 active, 1 deferred)")).isTrue();
        assertThat(logInterceptor.containsMessage(
                "Active transaction 0a.000b.0000000c: startScn=100, changeTime=2024-01-01T00:00:00Z, userName=DEBEZIUM, clientId=client-1, redoThread=1, events=5"))
                .isTrue();
        assertThat(logInterceptor.containsMessage(
                "Deferred transaction 0d.000e.0000000f: startScn=200, changeTime=2024-01-01T00:00:01Z, userName=OTHER, clientId=null, redoThread=2"))
                .isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2577")
    public void shouldSucceedWhenNoTransactionsArePending() throws Exception {
        final LogInterceptor logInterceptor = new LogInterceptor(LogPendingTransactionsAction.class);
        when(streamingSource.getPendingTransactions()).thenReturn(Collections.emptyList());

        final boolean result = action.arrived(createPayload(partition));

        assertThat(result).isTrue();
        assertThat(logInterceptor.containsMessage("0 total (0 active, 0 deferred)")).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2577")
    public void shouldRequestSynchronousInvocationBecauseTheBufferIsOwnedByTheStreamingThread() {
        assertThat(action.isSynchronous()).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2577")
    public void shouldReturnFalseWhenStreamingSourceNotAvailable() throws Exception {
        when(coordinator.getStreamingSource()).thenReturn(Optional.empty());

        final boolean result = action.arrived(createPayload(partition));

        assertThat(result).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#2577")
    public void shouldThrowExceptionWhenPartitionIsNotOracle() throws Exception {
        final TestPartition testPartition = new TestPartition();

        @SuppressWarnings("unchecked")
        final ChangeEventSourceCoordinator<TestPartition, ?> testCoordinator = (ChangeEventSourceCoordinator<TestPartition, ?>) (Object) coordinator;
        final LogPendingTransactionsAction<TestPartition> wrongAction = new LogPendingTransactionsAction<>(testCoordinator);

        assertThatThrownBy(() -> wrongAction.arrived(createPayload(testPartition)))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("is only supported by Oracle connector");
    }

    private static <P extends Partition> SignalPayload<P> createPayload(P partition) {
        return new SignalPayload<>(partition, "signal-id", LogPendingTransactionsAction.NAME, Document.create(), null, Collections.emptyMap());
    }

    private static class TestPartition implements Partition {
        @Override
        public Map<String, String> getSourcePartition() {
            return Collections.emptyMap();
        }
    }
}
