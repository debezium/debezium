/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.pipeline.signal.channels.SourceSignalChannel;

/**
 * Unit tests for {@link SignalDataCollectionValidationRequest#forConnector}.
 */
public class SignalDataCollectionValidationRequestTest {

    @Test
    public void shouldDeriveFieldsFromConnectorConfig() {
        MongoDbConnectorConfig connectorConfig = mock(MongoDbConnectorConfig.class);
        when(connectorConfig.getSignalingDataCollectionIds()).thenReturn(List.of("inventory.debezium_signal"));
        when(connectorConfig.isSignalDataCollectionValidationEnabled()).thenReturn(true);
        when(connectorConfig.getEnabledChannels()).thenReturn(List.of(SourceSignalChannel.CHANNEL_NAME));

        SignalDataCollectionValidationRequest request = SignalDataCollectionValidationRequest.forConnector(connectorConfig);

        assertThat(request.rawValues()).containsExactly("inventory.debezium_signal");
        assertThat(request.validationEnabled()).isTrue();
        assertThat(request.sourceChannelEnabled()).isTrue();
    }
}
