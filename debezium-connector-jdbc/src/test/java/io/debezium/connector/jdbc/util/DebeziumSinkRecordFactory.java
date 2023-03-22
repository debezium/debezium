/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.util;

/**
 * A {@link SinkRecordFactory} implementation that provides records that are structured based on Debezium's
 * complex envelope structure.
 *
 * @author Chris Cranford
 */
public class DebeziumSinkRecordFactory implements SinkRecordFactory {
    @Override
    public boolean isFlattened() {
        return false;
    }
}
