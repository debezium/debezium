/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

public class PreparedStatementQueryBinder implements QueryBinder {

    private final PreparedStatement binder;

    public PreparedStatementQueryBinder(PreparedStatement binder) {
        this.binder = binder;
    }

    @Override
    public void bind(ValueBindDescriptor valueBindDescriptor) {

        try {
            if (valueBindDescriptor.getBindableType() != null) { //TODO improve the naming
                binder.setObject(valueBindDescriptor.getIndex(), valueBindDescriptor.getValue(), Types.TIMESTAMP_WITH_TIMEZONE);
            } else {
                binder.setObject(valueBindDescriptor.getIndex(), valueBindDescriptor.getValue());
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
