/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

/**
 * The schema processor is responsible for periodically
 * refreshing the table schemas in Cassandra. Cassandra
 * CommitLog does not provide schema change as events,
 * so we pull the schema regularly for updates.
 */
public class SchemaProcessor extends AbstractProcessor {

    private static final String NAME = "Schema Processor";
    private final SchemaHolder schemaHolder;

    public SchemaProcessor(CassandraConnectorContext context) {
        super(NAME, context.getCassandraConnectorConfig().schemaPollIntervalMs().toMillis());
        schemaHolder = context.getSchemaHolder();
    }

    @Override
    public void process() {
        schemaHolder.refreshSchemas();
    }
}
