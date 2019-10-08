/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @deprecated Use {@link ExtractNewRecordState} instead. This class will be removed in a future release.
 */
@Deprecated
public class UnwrapFromEnvelope<R extends ConnectRecord<R>> extends ExtractNewRecordState<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(UnwrapFromEnvelope.class);

    public UnwrapFromEnvelope() {
        LOGGER.warn(
                "{} has been deprecated and is scheduled for removal. Use {} instead.",
                getClass().getSimpleName(),
                ExtractNewRecordState.class.getName());
    }
}
