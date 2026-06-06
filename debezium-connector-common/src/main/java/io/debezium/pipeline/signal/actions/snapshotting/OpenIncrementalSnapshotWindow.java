/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal.actions.snapshotting;

import io.debezium.pipeline.signal.SignalPayload;
import io.debezium.pipeline.signal.actions.SignalAction;
import io.debezium.pipeline.spi.Partition;

public class OpenIncrementalSnapshotWindow<P extends Partition> implements SignalAction<P> {

    public static final String NAME = "snapshot-window-open";

    public OpenIncrementalSnapshotWindow() {
    }

    @Override
    public boolean arrived(SignalPayload<P> signalPayload) {
        signalPayload.offsetContext.getIncrementalSnapshotContext().openWindow(signalPayload.id);
        return true;
    }

}
