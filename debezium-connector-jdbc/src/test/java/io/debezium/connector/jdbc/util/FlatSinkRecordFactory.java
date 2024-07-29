/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.util;

/**
 * A {@link SinkRecordFactory} implementation that provides records that have been flattened
 * mimic the behavior observed when the {@code ExtractNewRecordState} transformation is applied.
 *
 * @author Chris Cranford
 */
public class FlatSinkRecordFactory implements SinkRecordFactory {
    @Override
    public boolean isFlattened() {
        return true;
    }
}
