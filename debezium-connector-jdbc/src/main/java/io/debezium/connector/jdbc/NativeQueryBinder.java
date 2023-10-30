/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import org.hibernate.query.BindableType;
import org.hibernate.query.NativeQuery;
import org.hibernate.type.StandardBasicTypes;

public class NativeQueryBinder implements QueryBinder {

    private final NativeQuery<?> binder;

    public NativeQueryBinder(NativeQuery<?> binder) {
        this.binder = binder;
    }

    @Override
    public void bind(ValueBindDescriptor valueBindDescriptor) {

        if (valueBindDescriptor.getBindableType() != null) {
            binder.setParameter(valueBindDescriptor.getIndex(), ZonedDateTime.ofInstant(Instant.now(), ZoneOffset.UTC),
                    (BindableType) StandardBasicTypes.ZONED_DATE_TIME_WITH_TIMEZONE);
        }
        else {
            binder.setParameter(valueBindDescriptor.getIndex(), valueBindDescriptor.getValue());
        }
    }
}
