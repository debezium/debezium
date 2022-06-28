/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schema;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * A factory for creating {@link SchemaBuilder}.
 *
 * @author Anisha Mohanty
 */
public interface SchemaBuilderFactory {

    SchemaBuilder builder();

    default Schema schema() {
        return builder().build();
    }

    default Schema optionalSchema() {
        return builder().optional().build();
    }
}
