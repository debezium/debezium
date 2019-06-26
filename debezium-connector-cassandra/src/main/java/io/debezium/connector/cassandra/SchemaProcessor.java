/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The schema processor is responsible for periodically
 * refreshing the table schemas in Cassandra. Cassandra
 * CommitLog does not provide schema change as events,
 * so we pull the schema regularly for updates.
 */
public class SchemaProcessor extends AbstractProcessor {

    private static final String NAME = "Schema Processor";
    private static final int SLEEP_MS = 100;
    private final Duration schemaRefreshInterval;
    private final SchemaHolder schemaHolder;

    public SchemaProcessor(CassandraConnectorContext context, AtomicBoolean taskState) {
        super(NAME, taskState);
        schemaRefreshInterval = context.getCassandraConnectorConfig().schemaPollIntervalMs();
        schemaHolder = context.getSchemaHolder();
    }

    @Override
    public void doStart() throws InterruptedException {
        long then = System.currentTimeMillis();
        while (isTaskRunning()) {
            // Add a bit of time buffer so it isn't too busy looping
            Thread.sleep(SLEEP_MS);
            long now = System.currentTimeMillis();
            if (now - then >= schemaRefreshInterval.toMillis()) {
                refreshSchemas();
                then = now;
            }
        }
    }

    @Override
    public void doStop() {
        // do nothing
    }

    void refreshSchemas() {
        schemaHolder.refreshSchemas();
    }
}
