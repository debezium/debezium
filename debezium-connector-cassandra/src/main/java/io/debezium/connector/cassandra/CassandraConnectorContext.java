/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.util.Collections;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.CdcSourceTaskContext;

/**
 * Contains contextual information and objects scoped to the lifecycle
 * of {@link CassandraConnectorTask} implementation.
 */
public class CassandraConnectorContext extends CdcSourceTaskContext {
    private final CassandraConnectorConfig config;
    private final CassandraClient cassandraClient;
    private final ChangeEventQueue<Event> queue;
    private final SchemaHolder schemaHolder;
    private final OffsetWriter offsetWriter;

    public CassandraConnectorContext(CassandraConnectorConfig config) throws Exception {

        super(config.getContextName(), config.getLogicalName(), Collections::emptySet);
        this.config = config;

        try {
            // Loading up DDL schemas from disk
            loadDdlFromDisk(this.config.cassandraConfig());

            // Setting up Cassandra driver
            this.cassandraClient = new CassandraClient(this.config);

            // Setting up record queue ...
            this.queue = new ChangeEventQueue.Builder<Event>()
                    .pollInterval(this.config.pollIntervalMs())
                    .maxBatchSize(this.config.maxBatchSize())
                    .maxQueueSize(this.config.maxQueueSize())
                    .loggingContextSupplier(() -> this.configureLoggingContext(this.config.getContextName()))
                    .build();

            // Setting up schema holder ...
            this.schemaHolder = new SchemaHolder(this.cassandraClient, this.config.kafkaTopicPrefix(), this.config.getSourceInfoStructMaker());

            // Setting up a file-based offset manager ...
            this.offsetWriter = new FileOffsetWriter(this.config.offsetBackingStoreDir());
        }
        catch (Exception e) {
            // Clean up CassandraClient and FileOffsetWrite if connector context is failed to be initialized completely.
            cleanUp();
            throw e;
        }

    }

    /**
     * Initialize database using cassandra.yml config file. If initialization is successful,
     * load up non-system keyspace schema definitions from Cassandra.
     * @param yamlConfig the main config file path of a cassandra node
     */
    public void loadDdlFromDisk(String yamlConfig) {
        System.setProperty("cassandra.config", "file:///" + yamlConfig);
        if (!DatabaseDescriptor.isDaemonInitialized() && !DatabaseDescriptor.isToolInitialized()) {
            DatabaseDescriptor.toolInitialization();
            Schema.instance.loadFromDisk(false);
        }
    }

    public void cleanUp() {
        if (this.cassandraClient != null) {
            this.cassandraClient.close();
        }
        if (this.offsetWriter != null) {
            this.offsetWriter.close();
        }
    }

    public CassandraConnectorConfig getCassandraConnectorConfig() {
        return config;
    }

    public CassandraClient getCassandraClient() {
        return cassandraClient;
    }

    public ChangeEventQueue<Event> getQueue() {
        return queue;
    }

    public OffsetWriter getOffsetWriter() {
        return offsetWriter;
    }

    public SchemaHolder getSchemaHolder() {
        return schemaHolder;
    }
}
