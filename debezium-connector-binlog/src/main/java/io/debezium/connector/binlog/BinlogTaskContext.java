/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import com.github.shyiko.mysql.binlog.BinaryLogClient;

import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.relational.RelationalDatabaseSchema;

/**
 * Abstract implementation of {@link CdcSourceTaskContext} (state) for binlog-based connectors.
 *
 * @author Chris Cranford
 */
public class BinlogTaskContext<T extends RelationalDatabaseSchema> extends CdcSourceTaskContext {

    private final T schema;
    private final BinaryLogClient binaryLogClient;

    public BinlogTaskContext(BinlogConnectorConfig config, T schema) {
        super(config.getContextName(), config.getLogicalName(), config.getCustomMetricTags(), schema::tableIds);
        this.schema = schema;
        this.binaryLogClient = new BinaryLogClient(config.getHostName(), config.getPort(), config.getUserName(), config.getPassword());
    }

    /**
     * Get the task's database schema instance.
     *
     * @return the schema instance, never null
     */
    public T getSchema() {
        return schema;
    }

    /**
     * Get the task's underlying binary log client instance.
     *
     * @return the binary log client instance, never null
     */
    public BinaryLogClient getBinaryLogClient() {
        return binaryLogClient;
    }
}
