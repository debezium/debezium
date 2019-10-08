/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.snapshot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlwaysSnapshotter extends QueryingSnapshotter {

    private final static Logger LOGGER = LoggerFactory.getLogger(AlwaysSnapshotter.class);

    @Override
    public boolean shouldStream() {
        return true;
    }

    @Override
    public boolean shouldSnapshot() {
        LOGGER.info("Taking a new snapshot as per configuration");
        return true;
    }
}
