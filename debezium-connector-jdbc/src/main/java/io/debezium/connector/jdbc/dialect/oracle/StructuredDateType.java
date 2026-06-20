/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.oracle;

/**
 * Oracle implementation of {@link io.debezium.time.StructuredDate} values.
 */
public class StructuredDateType extends io.debezium.connector.jdbc.type.debezium.StructuredDateType {

    public static final StructuredDateType INSTANCE = new StructuredDateType();
}
