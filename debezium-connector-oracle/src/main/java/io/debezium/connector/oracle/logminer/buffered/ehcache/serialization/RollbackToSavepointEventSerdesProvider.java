/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.ehcache.serialization;

import io.debezium.connector.oracle.logminer.events.RollbackToSavepointEvent;

/**
 * A specialized implementation of {@link SerdesProvider} for {@link RollbackToSavepointEvent} types.
 *
 * @author Sergei Nikolaev
 */
public class RollbackToSavepointEventSerdesProvider<T extends RollbackToSavepointEvent> extends LogMinerEventSerdesProvider<T> {
    @Override
    public Class<?> getJavaType() {
        return RollbackToSavepointEvent.class;
    }
}
