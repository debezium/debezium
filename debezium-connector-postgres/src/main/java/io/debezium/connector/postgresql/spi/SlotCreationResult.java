/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.spi;

import org.postgresql.replication.LogSequenceNumber;

import io.debezium.common.annotation.Incubating;

/**
 * A simple data container representing the creation of a newly created replication slot.
 */
@Incubating
public class SlotCreationResult {

    private final String slotName;
    private final Long walStartLsn;
    private final String snapshotName;
    private final String pluginName;

    public SlotCreationResult(String name, String startLsn, String snapshotName, String pluginName) {
        this.slotName = name;
        this.walStartLsn = LogSequenceNumber.valueOf(startLsn).asLong();
        this.snapshotName = snapshotName;
        this.pluginName = pluginName;
    }

    /**
     * return the name of the created slot.
     */
    public String slotName() {
        return slotName;
    }

    public Long startLsn() {
        return walStartLsn;
    }

    public String snapshotName() {
        return snapshotName;
    }

    public String pluginName() {
        return pluginName;
    }
}
