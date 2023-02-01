/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.common;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SnapshotRecord;

public abstract class BaseSourceInfo extends AbstractSourceInfo {

    protected SnapshotRecord snapshotRecord;

    public BaseSourceInfo(CommonConnectorConfig config) {
        super(config);
    }

    public boolean isSnapshot() {
        return snapshotRecord != SnapshotRecord.INCREMENTAL && snapshotRecord != SnapshotRecord.FALSE;
    }

    /**
     * @param snapshot - TRUE if the source of even is snapshot phase, not the database log
     */
    public void setSnapshot(SnapshotRecord snapshot) {
        this.snapshotRecord = snapshot;
    }

    @Override
    public SnapshotRecord snapshot() {
        return snapshotRecord;
    }
}
