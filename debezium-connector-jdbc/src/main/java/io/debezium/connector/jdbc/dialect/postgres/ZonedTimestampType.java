/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import java.sql.Types;
import java.util.List;

import io.debezium.connector.jdbc.ValueBindDescriptor;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.time.ZonedTimestamp;

/**
 * An implementation of {@link Type} for {@link ZonedTimestamp} values specific to PostgreSQL.
 *
 * @author Mario Fiore Vitale
 */
public class ZonedTimestampType extends io.debezium.connector.jdbc.type.debezium.ZonedTimestampType {

    public static final ZonedTimestampType INSTANCE = new ZonedTimestampType();

    @Override
    protected List<ValueBindDescriptor> infinityTimestampValue(int index, Object value) {

        if (POSITIVE_INFINITY.equals(value)) {
            return List.of(new ValueBindDescriptor(index, POSITIVE_INFINITY, Types.VARCHAR));
        }
        else {
            return List.of(new ValueBindDescriptor(index, NEGATIVE_INFINITY, Types.VARCHAR));
        }
    }
}
