/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.processor.ehcache.serialization;

import io.debezium.connector.oracle.logminer.events.XmlEndEvent;

/**
 * A specialized implementation of {@link SerdesProvider} for {@link XmlEndEvent} types.
 *
 * @author Chris Cranford
 */
public class XmlEndEventSerdesProvider<T extends XmlEndEvent> extends LogMinerEventSerdesProvider<T> {
    @Override
    public Class<?> getJavaType() {
        return XmlEndEvent.class;
    }
}
