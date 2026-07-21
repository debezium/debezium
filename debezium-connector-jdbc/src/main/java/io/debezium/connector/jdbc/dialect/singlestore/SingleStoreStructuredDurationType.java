/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.singlestore;

/**
 * SingleStore implementation of structured duration values.
 */
public class SingleStoreStructuredDurationType extends io.debezium.connector.jdbc.dialect.mysql.StructuredDurationType {

    public static final SingleStoreStructuredDurationType INSTANCE = new SingleStoreStructuredDurationType();

    @Override
    protected int getTimeColumnSizeWithoutFraction() {
        // SingleStore reports TIME column size using its three-digit hour range.
        return 10;
    }
}
