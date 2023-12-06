/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.processors.spi;

import io.debezium.relational.RelationalDatabaseSchema;

/**
 * Contract that allows injecting a {@link RelationalDatabaseSchema}.
 *
 * @author Chris Cranford
 */
public interface RelationalDatabaseSchemaAware {
    void setDatabaseSchema(RelationalDatabaseSchema schema);
}
