/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.io.IOException;
import java.security.GeneralSecurityException;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;

/**
 * Contains contextual information and objects scoped to the lifecycle
 * of {@link CassandraConnectorTask} implementation.
 */
public class CassandraConnectorContext {
    private final CassandraConnectorConfig config;
    private final CassandraClient cassandraClient;
    private final BlockingEventQueue<Event> queue;
    private final SchemaHolder schemaHolder;
    private final OffsetWriter offsetWriter;

    public CassandraConnectorContext(CassandraConnectorConfig config) throws GeneralSecurityException, IOException {
        this.config = config;

        // Loading up DDL schemas from disk
        loadDdlFromDisk(this.config.cassandraConfig());

        // Setting up Cassandra driver
        this.cassandraClient = new CassandraClient(this.config);

        // Setting up record queue ...
        this.queue = new BlockingEventQueue<>(this.config.pollIntervalMs(), this.config.maxQueueSize(), this.config.maxBatchSize());

        // Setting up schema holder ...
        this.schemaHolder = new SchemaHolder(this.cassandraClient, this.config.kafkaTopicPrefix(), this.config.getSourceInfoStructMaker());

        // Setting up a file-based offset manager ...
        this.offsetWriter = new FileOffsetWriter(this.config.offsetBackingStoreDir());
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
        this.cassandraClient.close();
        this.offsetWriter.close();
    }

    public CassandraConnectorConfig getCassandraConnectorConfig() {
        return config;
    }

    public CassandraClient getCassandraClient() {
        return cassandraClient;
    }

    public BlockingEventQueue<Event> getQueue() {
        return queue;
    }

    public OffsetWriter getOffsetWriter() {
        return offsetWriter;
    }

    public SchemaHolder getSchemaHolder() {
        return schemaHolder;
    }
}
