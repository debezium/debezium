/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.List;

import io.debezium.pipeline.signal.channels.SourceSignalChannel;

/**
 * Plain inputs {@link SignalDataCollectionValidator#validate} needs, independent of {@link MongoDbConnectorConfig}
 * so it can be invoked outside a Kafka Connect connector too.
 *
 * @param rawValues the configured signal.data.collection values
 * @param validationEnabled whether validation should run at all
 * @param sourceChannelEnabled whether the source signal channel is enabled
 *
 * @author Debezium Authors
 */
public record SignalDataCollectionValidationRequest(List<String> rawValues, boolean validationEnabled, boolean sourceChannelEnabled) {

    public static SignalDataCollectionValidationRequest forConnector(MongoDbConnectorConfig connectorConfig) {
        return new SignalDataCollectionValidationRequest(connectorConfig.getSignalingDataCollectionIds(),
                connectorConfig.isSignalDataCollectionValidationEnabled(),
                connectorConfig.getEnabledChannels().contains(SourceSignalChannel.CHANNEL_NAME));
    }
}
