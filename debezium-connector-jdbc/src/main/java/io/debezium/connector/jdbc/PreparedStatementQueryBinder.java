/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class PreparedStatementQueryBinder implements QueryBinder {

    private final PreparedStatement binder;

    public PreparedStatementQueryBinder(PreparedStatement binder) {
        this.binder = binder;
    }

    @Override
    public void bind(ValueBindDescriptor valueBindDescriptor) {

        try {
            if (valueBindDescriptor.getTargetSqlType() != null) {
                binder.setObject(valueBindDescriptor.getIndex(), valueBindDescriptor.getValue(), valueBindDescriptor.getTargetSqlType());
            }
            else {
                binder.setObject(valueBindDescriptor.getIndex(), valueBindDescriptor.getValue());
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
