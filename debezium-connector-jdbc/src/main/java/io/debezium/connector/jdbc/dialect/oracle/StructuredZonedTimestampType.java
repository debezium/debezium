/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.oracle;

/**
 * Oracle implementation of {@link io.debezium.time.StructuredZonedTimestamp} values.
 */
public class StructuredZonedTimestampType extends io.debezium.connector.jdbc.type.debezium.StructuredZonedTimestampType {

    public static final StructuredZonedTimestampType INSTANCE = new StructuredZonedTimestampType();
}
