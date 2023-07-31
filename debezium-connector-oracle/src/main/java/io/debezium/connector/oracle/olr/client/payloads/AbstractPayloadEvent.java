/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.olr.client.payloads;

import io.debezium.connector.oracle.olr.client.PayloadEvent;

/**
 * Base class for all payload streaming changes.
 *
 * @author Chris Cranford
 */
public abstract class AbstractPayloadEvent implements PayloadEvent {

    private Integer num;
    private String rid;
    private Type type;

    public AbstractPayloadEvent(Type type) {
        this.type = type;
    }

    @Override
    public Type getType() {
        return type;
    }

    @Override
    public Integer getNum() {
        return num;
    }

    @Override
    public String getRid() {
        return rid;
    }

    @Override
    public String toString() {
        return "AbstractPayloadEvent{" +
                "type=" + type +
                ", num=" + num +
                ", rid='" + rid + '\'' +
                '}';
    }

}
