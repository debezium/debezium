/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.snapshot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.common.annotation.Incubating;

@Incubating
public class AlwaysSnapshotter extends AbstractSnapshotter {

    private final static Logger LOGGER = LoggerFactory.getLogger(AlwaysSnapshotter.class);

    @Override
    public boolean shouldSnapshot() {
        LOGGER.info("Taking a new snapshot as per configuration");
        return true;
    }

    @Override
    public boolean includeSchema() {
        return true;
    }

    @Override
    public boolean includeData() {
        return true;
    }

    @Override
    public boolean shouldStream() {
        return true;
    }
}
