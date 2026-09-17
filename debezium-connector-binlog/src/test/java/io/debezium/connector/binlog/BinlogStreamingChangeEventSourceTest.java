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

import org.junit.jupiter.api.Test;

import com.github.shyiko.mysql.binlog.network.SSLMode;
import com.github.shyiko.mysql.binlog.network.SSLSocketFactory;

import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.connector.binlog.jdbc.ConnectionConfiguration;

/**
 * Unit tests for {@link BinlogStreamingChangeEventSource#getBinlogSslSocketFactory}.
 */
public class BinlogStreamingChangeEventSourceTest {

    @Test
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

    private static BinlogStreamingChangeEventSource<?, ?> newSource(SSLMode sslMode) {
        final BinlogStreamingChangeEventSource<?, ?> source = mock(BinlogStreamingChangeEventSource.class, CALLS_REAL_METHODS);
        doReturn(sslMode).when(source).sslModeFor(any());
        return source;
    }
}
