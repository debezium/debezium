/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import io.debezium.schema.SchemaFactory;

/**
 * @author Chris Cranford
 */
public class MariaDbSchemaFactory extends SchemaFactory {
    private static final MariaDbSchemaFactory INSTANCE = new MariaDbSchemaFactory();

    public static MariaDbSchemaFactory get() {
        return INSTANCE;
    }
}
