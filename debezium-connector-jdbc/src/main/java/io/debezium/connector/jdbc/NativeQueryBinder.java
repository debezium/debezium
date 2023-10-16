/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import org.hibernate.query.BindableType;
import org.hibernate.query.NativeQuery;

public class NativeQueryBinder implements QueryBinder {

    private final NativeQuery<?> binder;

    public NativeQueryBinder(NativeQuery<?> binder) {
        this.binder = binder;
    }

    @Override
    public void bind(ValueBindDescriptor valueBindDescriptor) {

        if (valueBindDescriptor.getBindableType() != null) {
            binder.setParameter(valueBindDescriptor.getIndex(), valueBindDescriptor.getValue(), (BindableType) valueBindDescriptor.getBindableType());
        }
        else {
            binder.setParameter(valueBindDescriptor.getIndex(), valueBindDescriptor.getValue());
        }
    }
}
