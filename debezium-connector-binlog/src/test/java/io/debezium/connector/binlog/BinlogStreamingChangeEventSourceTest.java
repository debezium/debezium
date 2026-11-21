/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Field;
import java.security.KeyStore;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.network.SSLMode;
import com.github.shyiko.mysql.binlog.network.SSLSocketFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.binlog.BinlogStreamingChangeEventSource.BinlogPosition;
import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.connector.binlog.jdbc.ConnectionConfiguration;
import io.debezium.doc.FixFor;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.util.Clock;

/**
 * Unit tests for {@link BinlogStreamingChangeEventSource#getBinlogSslSocketFactory} and for the rewind of the binary log client.
 */
public class BinlogStreamingChangeEventSourceTest {

    private static final long CONNECTION_TIMEOUT_MS = 30_000L;

    @Test
    @FixFor("debezium/dbz#2914")
    public void shouldUseTheTrackedPositionOnlyForTheRewindReconnect() throws Exception {
        final RewindFixture fixture = new RewindFixture();
        when(fixture.context.isRunning()).thenReturn(true);
        when(fixture.client.isKeepAlive()).thenReturn(true);
        when(fixture.client.isConnected()).thenReturn(false);

        fixture.source.reconnectAtPosition(fixture.context, new BinlogPosition("mysql-bin.000007", 1234L));

        final InOrder order = inOrder(fixture.client);
        order.verify(fixture.client).disconnect();
        order.verify(fixture.client).setBinlogFilename("mysql-bin.000007");
        order.verify(fixture.client).setBinlogPosition(1234L);
        order.verify(fixture.client).setUseBinlogFilenamePositionInGtidMode(true);
        order.verify(fixture.client).connect(CONNECTION_TIMEOUT_MS);
        // reset as soon as the dump command is sent, not after waiting for the keepalive thread of the new connection:
        // a reconnect by the keepalive thread must resume from the GTID set, not from a position inside a transaction
        order.verify(fixture.client).setUseBinlogFilenamePositionInGtidMode(false);
        order.verify(fixture.client).isConnected();
    }

    @Test
    @FixFor("debezium/dbz#2914")
    public void shouldResetTheTrackedPositionFlagAndFailTheTaskWhenTheRewindReconnectFails() throws Exception {
        final RewindFixture fixture = new RewindFixture();
        when(fixture.context.isRunning()).thenReturn(true);
        doThrow(new IOException("Access denied")).when(fixture.client).connect(anyLong());

        assertThatThrownBy(() -> fixture.source.reconnectAtPosition(fixture.context, new BinlogPosition("mysql-bin.000007", 1234L)))
                .isInstanceOf(DebeziumException.class)
                .hasCauseInstanceOf(IOException.class);

        final InOrder order = inOrder(fixture.client);
        order.verify(fixture.client).setUseBinlogFilenamePositionInGtidMode(true);
        order.verify(fixture.client).connect(CONNECTION_TIMEOUT_MS);
        order.verify(fixture.client).setUseBinlogFilenamePositionInGtidMode(false);
    }

    @Test
    @FixFor("debezium/dbz#2914")
    public void shouldResetTheTrackedPositionFlagAndNotFailWhenTheRewindReconnectIsInterruptedByShutdown() throws Exception {
        final RewindFixture fixture = new RewindFixture();
        // running when the rewind starts, stopped by the time the reconnect fails
        when(fixture.context.isRunning()).thenReturn(true, false);
        doThrow(new TimeoutException()).when(fixture.client).connect(anyLong());

        fixture.source.reconnectAtPosition(fixture.context, new BinlogPosition("mysql-bin.000007", 1234L));

        verify(fixture.client).setUseBinlogFilenamePositionInGtidMode(true);
        verify(fixture.client).setUseBinlogFilenamePositionInGtidMode(false);
    }

    @Test
    @FixFor("debezium/dbz#2914")
    public void shouldNotReconnectForARewindWhenTheConnectorIsStopping() throws Exception {
        final RewindFixture fixture = new RewindFixture();
        when(fixture.context.isRunning()).thenReturn(false);

        fixture.source.reconnectAtPosition(fixture.context, new BinlogPosition("mysql-bin.000007", 1234L));

        verifyNoInteractions(fixture.client);
    }

    @Test
    @FixFor("debezium/dbz#2914")
    public void shouldDisconnectAndQueueTheRewindPosition() throws Exception {
        final RewindFixture fixture = new RewindFixture();
        when(fixture.context.isRunning()).thenReturn(true);
        final BinlogPosition position = new BinlogPosition("mysql-bin.000007", 1234L);

        fixture.source.rewindBinaryLogClient(fixture.context, position);

        // the channel must be closed on the calling thread, the reconnect is left to the thread that runs execute()
        verify(fixture.client).disconnect();
        verify(fixture.client, never()).connect(anyLong());
        assertThat(fixture.rewindRequests).containsExactly(position);
    }

    @Test
    @FixFor("debezium/dbz#2914")
    public void shouldNotQueueARewindWhenTheConnectorIsStopping() {
        final RewindFixture fixture = new RewindFixture();
        when(fixture.context.isRunning()).thenReturn(false);

        fixture.source.rewindBinaryLogClient(fixture.context, new BinlogPosition("mysql-bin.000007", 1234L));

        verifyNoInteractions(fixture.client);
        assertThat(fixture.rewindRequests).isEmpty();
    }

    @Test
    @FixFor("debezium/dbz#2655")
    public void shouldTreatBlankKeystoreAndTruststorePathsAsUnset() {
        final ConnectionConfiguration connectionConfig = mock(ConnectionConfiguration.class);
        when(connectionConfig.sslKeyStore()).thenReturn("");
        when(connectionConfig.sslTrustStore()).thenReturn("");

        final BinlogConnectorConnection connection = mock(BinlogConnectorConnection.class);
        when(connection.getSessionVariableForSslVersion()).thenReturn("TLSv1.2");
        when(connection.connectionConfig()).thenReturn(connectionConfig);

        final SSLSocketFactory factory = newSource(SSLMode.PREFERRED)
                .getBinlogSslSocketFactory(mock(BinlogConnectorConfig.class), connection);

        // A blank path is treated as unset, so no keystore is loaded and the task is not failed.
        assertThat(factory).isNotNull();
        verify(connection, never()).loadKeyStore(any(), any());
    }

    @Test
    @FixFor("debezium/dbz#2655")
    public void shouldLoadKeystoreWhenPathIsConfigured() throws Exception {
        final KeyStore emptyKeyStore = KeyStore.getInstance("JKS");
        emptyKeyStore.load(null, null);

        final ConnectionConfiguration connectionConfig = mock(ConnectionConfiguration.class);
        when(connectionConfig.sslKeyStore()).thenReturn("/path/to/keystore");
        when(connectionConfig.sslTrustStore()).thenReturn("");

        final BinlogConnectorConnection connection = mock(BinlogConnectorConnection.class);
        when(connection.getSessionVariableForSslVersion()).thenReturn("TLSv1.2");
        when(connection.connectionConfig()).thenReturn(connectionConfig);
        when(connection.loadKeyStore(any(), any())).thenReturn(emptyKeyStore);

        newSource(SSLMode.PREFERRED)
                .getBinlogSslSocketFactory(mock(BinlogConnectorConfig.class), connection);

        verify(connection, times(1)).loadKeyStore(eq("/path/to/keystore"), any());
    }

    /**
     * A source with only what the rewind needs: a mocked binary log client, the connection timeout and the queue of rewind requests.
     */
    private static class RewindFixture {
        final BinlogStreamingChangeEventSource<?, ?> source = mock(BinlogStreamingChangeEventSource.class, CALLS_REAL_METHODS);
        final BinaryLogClient client = mock(BinaryLogClient.class);
        final ChangeEventSourceContext context = mock(ChangeEventSourceContext.class);
        final BlockingQueue<BinlogPosition> rewindRequests = new LinkedBlockingQueue<>();

        RewindFixture() {
            final BinlogConnectorConfig connectorConfig = mock(BinlogConnectorConfig.class);
            when(connectorConfig.getConnectionTimeout()).thenReturn(Duration.ofMillis(CONNECTION_TIMEOUT_MS));
            set("client", client);
            set("connectorConfig", connectorConfig);
            set("rewindRequests", rewindRequests);
            set("binaryLogClientThreads", new ConcurrentHashMap<String, Thread>());
            set("clock", Clock.SYSTEM);
            set("errorHandler", mock(ErrorHandler.class));
        }

        private void set(String fieldName, Object value) {
            try {
                final Field field = BinlogStreamingChangeEventSource.class.getDeclaredField(fieldName);
                field.setAccessible(true);
                field.set(source, value);
            }
            catch (ReflectiveOperationException e) {
                throw new IllegalStateException("Unable to set " + fieldName, e);
            }
        }
    }

    private static BinlogStreamingChangeEventSource<?, ?> newSource(SSLMode sslMode) {
        final BinlogStreamingChangeEventSource<?, ?> source = mock(BinlogStreamingChangeEventSource.class, CALLS_REAL_METHODS);
        doReturn(sslMode).when(source).sslModeFor(any());
        return source;
    }
}
