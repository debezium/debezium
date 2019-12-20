/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.quarkus.outbox;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Describes an event that should be exported via the "outbox" table.
 *
 * @author Chris Cranford
 */
public interface ExportedEvent {
    /**
     * The id of the aggregate affected by a given event.  For example, the order id in case of events
     * relating to an order, or order lines of that order.  This is used to ensure ordering of events
     * within an aggregate type.
     */
    String getAggregateId();

    /**
     * The type of the aggregate affected by the event.  For example, "order" in case of events relating
     * to an order, or order lines of that order.  This is used as the topic name.
     */
    String getAggregateType();

    /**
     * The type of an event.  For example, "Order Created" or "Order Line Cancelled" for events that
     * belong to an given aggregate type such as "order".
     */
    String getType();

    /**
     * The timestamp at which the event occurred.
     */
    Long getTimestamp();

    /**
     * The event payload.
     */
    JsonNode getPayload();
}
