/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.relational.RelationalDatabaseSchema;

/**
 * Abstract implementation of {@link CdcSourceTaskContext} (state) for binlog-based connectors.
 *
 * @author Chris Cranford
 */
public class BinlogTaskContext<T extends RelationalDatabaseSchema> extends CdcSourceTaskContext {

    public BinlogTaskContext(BinlogConnectorConfig config) {
        super(config, config.getCustomMetricTags());
    }
}
