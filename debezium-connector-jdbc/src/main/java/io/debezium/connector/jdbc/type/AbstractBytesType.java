/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.ValueBindDescriptor;

/**
 * An abstract {@link Type} implementation for {@code BYTES} column types.
 *
 * @author Chris Cranford
 */
public abstract class AbstractBytesType extends AbstractType {

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ "BYTES" };
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value instanceof ByteBuffer) {
            return List.of(new ValueBindDescriptor(index, ((ByteBuffer) value).array()));
        }
        return super.bind(index, schema, value);
    }
}
