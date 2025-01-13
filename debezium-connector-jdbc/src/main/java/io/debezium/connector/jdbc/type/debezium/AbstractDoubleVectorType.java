/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.debezium;

import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.data.vector.DoubleVector;

/**
 * Abstract base class for double-based vector field types.
 *
 * For targets that do not support vector data types, values will be serialized based on the Kafka schema type,
 * which by default is {@code ARRAY}.
 *
 * @author Chris Cranford
 */
public abstract class AbstractDoubleVectorType extends AbstractType {
    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ DoubleVector.LOGICAL_NAME };
    }
}
