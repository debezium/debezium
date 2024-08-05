/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.ehcache.serialization;

import io.debezium.connector.oracle.logminer.events.TruncateEvent;

/**
 * A specialized implementation of {@link SerdesProvider} for {@link TruncateEvent} types.
 *
 * @author Chris Cranford
 */
public class TruncateEventSerdesProvider<T extends TruncateEvent> extends DmlEventSerdesProvider<T> {
    @Override
    public Class<?> getJavaType() {
        return TruncateEvent.class;
    }
}
