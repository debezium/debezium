/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.signal.Signal.Payload;
import io.debezium.pipeline.spi.Partition;

public class Log<P extends Partition> implements Signal.Action<P> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Log.class);
    private static final String FIELD_MESSAGE = "message";

    public static final String NAME = "log";

    @Override
    public boolean arrived(Payload<P> signalPayload) {
        final String message = signalPayload.data.getString(FIELD_MESSAGE);
        if (message == null || message.isEmpty()) {
            LOGGER.warn("Logging signal '{}' has arrived but the requested field '{}' is missing from data", signalPayload, FIELD_MESSAGE);
            return false;
        }
        LOGGER.info(message, signalPayload.offsetContext != null ? signalPayload.offsetContext.getOffset() : "<none>");
        return true;
    }

}
