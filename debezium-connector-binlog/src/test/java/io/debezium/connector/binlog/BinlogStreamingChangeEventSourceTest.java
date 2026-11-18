/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.security.KeyStore;
import java.time.Duration;

import org.junit.jupiter.api.Test;

import com.github.shyiko.mysql.binlog.network.SSLMode;
import com.github.shyiko.mysql.binlog.network.SSLSocketFactory;

import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.connector.binlog.jdbc.ConnectionConfiguration;
import io.debezium.doc.FixFor;

/**
 * Unit tests for {@link BinlogStreamingChangeEventSource}.
 */
public class BinlogStreamingChangeEventSourceTest {

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

    @Test
    @FixFor("debezium/dbz#1328")
    public void shouldKeepTheKeepAlivePeriodWhenTheHeartbeatIsDisabled() {
        assertThat(BinlogStreamingChangeEventSource.serverHeartbeatPeriod(Duration.ofMillis(0), 48_000))
                .isEqualTo(48_000);
    }

    @Test
    @FixFor("debezium/dbz#1328")
    public void shouldFloorTheServerHeartbeatPeriodAtTheMinimumPeriod() {
        assertThat(BinlogStreamingChangeEventSource.serverHeartbeatPeriod(Duration.ofMillis(100), 48_000))
                .isEqualTo(50);
    }

    @Test
    @FixFor("debezium/dbz#1328")
    public void shouldCapTheServerHeartbeatPeriodAtTheKeepAlivePeriod() {
        assertThat(BinlogStreamingChangeEventSource.serverHeartbeatPeriod(Duration.ofMillis(300_000), 8_000))
                .isEqualTo(8_000);
    }

    @Test
    @FixFor("debezium/dbz#1328")
    public void shouldRequestAnEighthOfTheHeartbeatInterval() {
        assertThat(BinlogStreamingChangeEventSource.serverHeartbeatPeriod(Duration.ofMillis(1_000), 48_000))
                .isEqualTo(125);
    }

    private static BinlogStreamingChangeEventSource<?, ?> newSource(SSLMode sslMode) {
        final BinlogStreamingChangeEventSource<?, ?> source = mock(BinlogStreamingChangeEventSource.class, CALLS_REAL_METHODS);
        doReturn(sslMode).when(source).sslModeFor(any());
        return source;
    }
}
