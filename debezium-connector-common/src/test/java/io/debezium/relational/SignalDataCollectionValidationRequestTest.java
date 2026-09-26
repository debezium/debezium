/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.signal.channels.SourceSignalChannel;
import io.debezium.relational.Tables.ColumnNameFilter;

/**
 * Unit tests for {@link SignalDataCollectionValidationRequest#forConnector}.
 */
public class SignalDataCollectionValidationRequestTest {

    private RelationalDatabaseConnectorConfig connectorConfig(String snapshotMode) {
        RelationalDatabaseConnectorConfig connectorConfig = mock(RelationalDatabaseConnectorConfig.class);
        when(connectorConfig.getSignalingDataCollectionIds()).thenReturn(List.of("dbo.debezium_signal"));
        when(connectorConfig.isSignalDataCollectionValidationEnabled()).thenReturn(true);
        when(connectorConfig.getSnapshotMode()).thenReturn(() -> snapshotMode);
        when(connectorConfig.getEnabledChannels()).thenReturn(List.of(SourceSignalChannel.CHANNEL_NAME));
        when(connectorConfig.getColumnFilter()).thenReturn((catalog, schema, table, column) -> true);
        return connectorConfig;
    }

    @Test
    public void shouldDeriveFieldsFromConnectorConfig() {
        RelationalDatabaseConnectorConfig connectorConfig = connectorConfig("streaming");
        ColumnNameFilter columnFilter = connectorConfig.getColumnFilter();
        Function<String, JdbcConnection> connectionResolver = rawValue -> mock(JdbcConnection.class);

        SignalDataCollectionValidationRequest request = SignalDataCollectionValidationRequest.forConnector(connectorConfig, connectionResolver);

        assertThat(request.rawValues()).containsExactly("dbo.debezium_signal");
        assertThat(request.validationEnabled()).isTrue();
        assertThat(request.streamingCapable()).isTrue();
        assertThat(request.sourceChannelEnabled()).isTrue();
        assertThat(request.connectionResolver()).isSameAs(connectionResolver);
        assertThat(request.columnFilter()).isSameAs(columnFilter);
    }

    @Test
    public void shouldMarkNotStreamingCapableWhenSnapshotModeIsInitialOnly() {
        SignalDataCollectionValidationRequest request = SignalDataCollectionValidationRequest.forConnector(
                connectorConfig("initial_only"), rawValue -> mock(JdbcConnection.class));

        assertThat(request.streamingCapable()).isFalse();
    }
}
