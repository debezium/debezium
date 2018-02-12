/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.util.Clock;

/**
 * @author Randall Hauch
 *
 */
public class ReplicationContext extends ConnectionContext {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Filters filters;
    private final SourceInfo source;
    private final Clock clock = Clock.system();
    private final TopicSelector topicSelector;
    private final boolean emitTombstoneOnDelete;

    /**
     * @param config the configuration
     */
    public ReplicationContext(Configuration config) {
        super(config);

        final String serverName = config.getString(MongoDbConnectorConfig.LOGICAL_NAME);
        this.filters = new Filters(config);
        this.source = new SourceInfo(serverName);
        this.topicSelector = TopicSelector.defaultSelector(serverName);
        this.emitTombstoneOnDelete = config.getBoolean(CommonConnectorConfig.TOMBSTONES_ON_DELETE);
    }

    @Override
    protected Logger logger() {
        return logger;
    }

    public TopicSelector topicSelector() {
        return topicSelector;
    }

    public Predicate<String> databaseFilter() {
        return filters.databaseFilter();
    }

    public Predicate<CollectionId> collectionFilter() {
        return filters.collectionFilter();
    }

    public SourceInfo source() {
        return source;
    }

    public Clock clock() {
        return clock;
    }

    public boolean isEmitTombstoneOnDelete() {
        return emitTombstoneOnDelete;
    }
}
