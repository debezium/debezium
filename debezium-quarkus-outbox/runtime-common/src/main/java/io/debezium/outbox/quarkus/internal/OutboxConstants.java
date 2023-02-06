/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.internal;

/**
 * Constant values used by the Debezium outbox Quarkus extension.
 *
 * @author Chris Cranford
 */
public final class OutboxConstants {

    public static final String OUTBOX_ENTITY_HBMXML = "META-INF/OutboxEvent.hbm.xml";
    public static final String OUTBOX_ENTITY_FULLNAME = "io.debezium.outbox.quarkus.internal.OutboxEvent";

    public static final String TRACING_SPAN_CONTEXT = "tracingspancontext";

    private OutboxConstants() {
    }
}
