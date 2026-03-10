/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal.channels;

import java.util.List;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.signal.SignalRecord;

/**
 * This interface is used to provide custom read channels for the Debezium signaling feature:
 *
 * Implementations must:
 * define the name of the reader in {@link #name()},
 * initialize specific configuration/variables/connections in the {@link #init(CommonConnectorConfig connectorConfig)} method,
 * implement reset logic for specific channel in the {@link #reset(Object)} method if you need to reset already processed signals,
 * provide a list of signal record in the {@link #read()} method. It is called by {@link SignalProcessor} in a thread loop
 * Close all allocated resources int the {@link #close()} method.
 *
 * @author Mario Fiore Vitale
 */
public interface SignalChannelReader {
    String name();

    void init(CommonConnectorConfig connectorConfig);

    default <T> void reset(T reference) {
    }

    List<SignalRecord> read();

    void close();
}
