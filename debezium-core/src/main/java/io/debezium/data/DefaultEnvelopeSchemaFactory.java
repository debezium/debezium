/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.data;

import io.debezium.schema.SchemaFactory;

/**
 * Default implementation of {@link EnvelopeSchemaFactory}.
 *
 * <p>This class delegates to {@link SchemaFactory#datatypeEnvelopeSchema()}.
 *
 * @author Debezium Authors
 * @see EnvelopeSchemaFactory
 */
public class DefaultEnvelopeSchemaFactory implements EnvelopeSchemaFactory {

    /**
     * Returns an {@link Envelope.Builder} backed by the standard Debezium
     * {@link SchemaFactory#datatypeEnvelopeSchema()} builder.
     *
     * @return a new standard Debezium envelope builder; never {@code null}
     */
    @Override
    public Envelope.Builder createEnvelopeSchema() {
        return SchemaFactory.get().datatypeEnvelopeSchema();
    }
}
