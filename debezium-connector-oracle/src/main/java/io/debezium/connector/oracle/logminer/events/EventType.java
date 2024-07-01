/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.events;

import java.io.Serializable;

/**
 * Represents all supported event types that are loaded from Oracle LogMiner.
 *
 * @author Chris Cranford
 */
public enum EventType implements Serializable {
    INSERT(1),
    DELETE(2),
    UPDATE(3),
    DDL(5),
    START(6),
    COMMIT(7),
    SELECT_LOB_LOCATOR(9),
    LOB_WRITE(10),
    LOB_TRIM(11),
    LOB_ERASE(29),
    MISSING_SCN(34),
    ROLLBACK(36),
    XML_BEGIN(68),
    XML_WRITE(70),
    XML_END(71),
    UNSUPPORTED(255);

    private static EventType[] types = new EventType[256];

    static {
        for (EventType option : EventType.values()) {
            types[option.getValue()] = option;
        }
    }

    private static final long serialVersionUID = 1L;

    private int value;

    EventType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    /**
     * Resolve an EventType from a numeric event type operation code.
     *
     * @param value the operation code
     * @return the event type, will be {@link #UNSUPPORTED} if the code is not supported.
     */
    public static EventType from(int value) {
        return value < types.length ? types[value] : EventType.UNSUPPORTED;
    }
}
