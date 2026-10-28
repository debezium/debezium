/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.mysql;

/**
 * MariaDB implementation of structured duration values.
 */
public class MariaDbStructuredDurationType extends StructuredDurationType {

    public static final MariaDbStructuredDurationType INSTANCE = new MariaDbStructuredDurationType();

    @Override
    protected boolean isFractionAllowedAtTimeLimit() {
        // MariaDB TIME columns accept values up to 838:59:59.999999
        return true;
    }
}
