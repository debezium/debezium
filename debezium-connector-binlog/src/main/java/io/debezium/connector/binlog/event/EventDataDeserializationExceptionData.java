/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.event;

import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDataDeserializationException;

/**
 * Event data for an event of type {@link com.github.shyiko.mysql.binlog.event.EventType#INCIDENT} that
 * represents a failure to deserialize a binlog event.
 *
 * @author Gunnar Morling
 */
public class EventDataDeserializationExceptionData implements EventData {

    private static final long serialVersionUID = 1L;

    private final EventDataDeserializationException cause;

    public EventDataDeserializationExceptionData(EventDataDeserializationException cause) {
        this.cause = cause;
    }

    @Override
    public String toString() {
        return "EventDataDeserializationExceptionData [cause=" + cause + "]";
    }

    public EventDataDeserializationException getCause() {
        return cause;
    }
}
