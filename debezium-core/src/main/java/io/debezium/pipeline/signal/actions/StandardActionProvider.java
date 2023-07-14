/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal.actions;

import java.util.Map;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.signal.actions.snapshotting.CloseIncrementalSnapshotWindow;
import io.debezium.pipeline.signal.actions.snapshotting.ExecuteSnapshot;
import io.debezium.pipeline.signal.actions.snapshotting.OpenIncrementalSnapshotWindow;
import io.debezium.pipeline.signal.actions.snapshotting.PauseIncrementalSnapshot;
import io.debezium.pipeline.signal.actions.snapshotting.ResumeIncrementalSnapshot;
import io.debezium.pipeline.signal.actions.snapshotting.StopSnapshot;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.history.JsonTableChangeSerializer;
import io.debezium.spi.schema.DataCollectionId;

public class StandardActionProvider implements SignalActionProvider {
    @Override
    public <P extends Partition> Map<String, SignalAction<P>> createActions(EventDispatcher<P, ? extends DataCollectionId> dispatcher,
                                                                            ChangeEventSourceCoordinator<P, ?> changeEventSourceCoordinator,
                                                                            CommonConnectorConfig connectorConfig) {
        return Map.of(Log.NAME, new Log<>(),
                SchemaChanges.NAME, new SchemaChanges<>(dispatcher, connectorConfig, new JsonTableChangeSerializer()),
                ExecuteSnapshot.NAME, new ExecuteSnapshot<>(dispatcher, changeEventSourceCoordinator),
                StopSnapshot.NAME, new StopSnapshot<>(dispatcher),
                OpenIncrementalSnapshotWindow.NAME, new OpenIncrementalSnapshotWindow<>(),
                CloseIncrementalSnapshotWindow.NAME, new CloseIncrementalSnapshotWindow<>(dispatcher),
                PauseIncrementalSnapshot.NAME, new PauseIncrementalSnapshot<>(dispatcher),
                ResumeIncrementalSnapshot.NAME, new ResumeIncrementalSnapshot<>(dispatcher));
    }
}
