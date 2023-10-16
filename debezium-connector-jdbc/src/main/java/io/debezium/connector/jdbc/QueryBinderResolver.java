/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import java.sql.PreparedStatement;

import org.apache.kafka.connect.errors.ConnectException;
import org.hibernate.query.NativeQuery;

public class QueryBinderResolver {

    public QueryBinderResolver() {

    }

    public QueryBinder resolve(Object binder) {

        if (binder instanceof NativeQuery<?>) {
            return new NativeQueryBinder((NativeQuery<?>) binder);
        }

        if (binder instanceof PreparedStatement) {
            return new PreparedStatementQueryBinder((PreparedStatement) binder);
        }

        throw new ConnectException(String.format("No binder found for %s", binder));
    }
}
