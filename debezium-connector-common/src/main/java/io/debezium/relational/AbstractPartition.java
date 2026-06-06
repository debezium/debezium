/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.Collections;
import java.util.Map;

import io.debezium.pipeline.spi.Partition;
import io.debezium.util.LoggingContext;

/**
 * An abstract implementation of {@link io.debezium.pipeline.spi.Partition} which provides default facilities for logging.
 *
 * @author vjuranek
 */
public abstract class AbstractPartition implements Partition {

    protected final String databaseName;

    public AbstractPartition(String databaseName) {
        this.databaseName = databaseName;
    }

    @Override
    public Map<String, String> getLoggingContext() {
        return Collections.singletonMap(LoggingContext.DATABASE_NAME, databaseName);
    }
}
