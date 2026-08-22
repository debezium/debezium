/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.data;

/**
 * A factory interface that provides the {@link Envelope.Builder} used to construct the Kafka Connect
 * schema for a Debezium change event envelope.
 *
 * <p>By default, Debezium uses {@link DefaultEnvelopeSchemaFactory} which produces the standard
 * CDC envelope ({@code before}, {@code after}, {@code source}, {@code op}, {@code ts_ms}, etc.).
 *
 * <p>Custom implementations may be provided via the {@code envelope.schema.factory} connector
 * configuration property. This allows sinks that do not require the full CDC format (e.g., Apache
 * Iceberg sinks) to define the envelope layout upfront, avoiding the overhead of post-processing
 * SMTs (Single Message Transforms) that reconstruct the schema after the fact.
 *
 * <p>Usage example — creating a custom factory inline:
 * <pre>{@code
 * EnvelopeSchemaFactory myFactory = () -> Envelope.defineSchema().withDoc("Iceberg-optimized envelope");
 *
 * Envelope envelope = Envelope.defineSchema(myFactory)
 *     .withName("my.topic.Envelope")
 *     .withRecord(tableSchema)
 *     .withSource(sourceSchema)
 *     .build();
 * }</pre>
 *
 * @author Debezium Authors
 */
@FunctionalInterface
public interface EnvelopeSchemaFactory {

    /**
     * Creates and returns an {@link Envelope.Builder} that will be used to construct the envelope schema.
     *
     * <p>Implementations may return a modified builder (e.g., one that skips certain fields or
     * adds custom fields). The caller is responsible for chaining the remaining builder calls
     * ({@code withName}, {@code withRecord}, {@code withSource}, {@code build()}) after this method returns.
     *
     * @return a new {@link Envelope.Builder}; never {@code null}
     */
    Envelope.Builder createEnvelopeSchema();
}
