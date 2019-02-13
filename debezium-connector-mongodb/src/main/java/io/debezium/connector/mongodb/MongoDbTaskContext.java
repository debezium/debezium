/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.Collections;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.mongodb.MongoDbConnectorConfig.SnapshotMode;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.schema.TopicSelector;

/**
 * @author Randall Hauch
 *
 */
public class MongoDbTaskContext extends CdcSourceTaskContext {

    private final Filters filters;
    private final SourceInfo source;
    private final TopicSelector<CollectionId> topicSelector;
    private final boolean emitTombstoneOnDelete;
    private final String serverName;
    private final ConnectionContext connectionContext;
    private final String snapshotMode;

    /**
     * @param config the configuration
     */
    public MongoDbTaskContext(Configuration config) {
        super("MongoDB", config.getString(MongoDbConnectorConfig.LOGICAL_NAME), Collections::emptySet);

        final String serverName = config.getString(MongoDbConnectorConfig.LOGICAL_NAME);
        this.filters = new Filters(config);
        this.source = new SourceInfo(serverName);
        this.topicSelector = MongoDbTopicSelector.defaultSelector(serverName, config.getString(Heartbeat.HEARTBEAT_TOPICS_PREFIX));
        this.emitTombstoneOnDelete = config.getBoolean(CommonConnectorConfig.TOMBSTONES_ON_DELETE);
        this.serverName = config.getString(MongoDbConnectorConfig.LOGICAL_NAME);
        this.connectionContext = new ConnectionContext(config);
        this.snapshotMode = config.getString(MongoDbConnectorConfig.SNAPSHOT_MODE);
    }

    public TopicSelector<CollectionId> topicSelector() {
        return topicSelector;
    }

    public Filters filters() {
        return filters;
    }

    public SourceInfo source() {
        return source;
    }

    public boolean isEmitTombstoneOnDelete() {
        return emitTombstoneOnDelete;
    }

    public String serverName() {
        return serverName;
    }

    public ConnectionContext getConnectionContext() {
        return connectionContext;
    }

    public boolean isSnapshotAllowed() {
        return snapshotMode() == SnapshotMode.INITIAL;
    }

    public boolean isSnapshotNeverAllowed() {
        return snapshotMode() == SnapshotMode.NEVER;
    }

    protected SnapshotMode snapshotMode() {
        String value = this.snapshotMode;
        return SnapshotMode.parse(value, MongoDbConnectorConfig.SNAPSHOT_MODE.defaultValueAsString());
    }
}
