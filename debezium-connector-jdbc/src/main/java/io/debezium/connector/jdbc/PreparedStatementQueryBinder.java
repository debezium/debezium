/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import java.io.StringReader;
import java.sql.Array;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.sink.valuebinding.ValueBindDescriptor;

public class PreparedStatementQueryBinder implements QueryBinder {

    private static final Logger LOGGER = LoggerFactory.getLogger(PreparedStatementQueryBinder.class);

    private final PreparedStatement binder;

    public PreparedStatementQueryBinder(PreparedStatement binder) {
        this.binder = binder;
    }

    @Override
    public void bind(ValueBindDescriptor valueBindDescriptor) {
        try {
            if (valueBindDescriptor.getTargetSqlType() != null) {
                if (valueBindDescriptor.getValue() == null) {
                    LOGGER.trace("Binding parameter #{} as NULL with Jdbc Type {}", valueBindDescriptor.getIndex(), valueBindDescriptor.getTargetSqlType());
                    binder.setNull(valueBindDescriptor.getIndex(), valueBindDescriptor.getTargetSqlType());
                    return;
                }
                switch (valueBindDescriptor.getTargetSqlType()) {
                    case Types.ARRAY -> {
                        LOGGER.trace("Binding parameter #{} as ARRAY", valueBindDescriptor.getIndex());
                        Collection<Object> collection = (Collection<Object>) valueBindDescriptor.getValue();
                        Array array = binder.getConnection().createArrayOf(valueBindDescriptor.getElementTypeName(), collection.toArray());
                        binder.setArray(valueBindDescriptor.getIndex(), array);
                    }
                    case Types.CLOB, Types.LONGVARCHAR -> {
                        LOGGER.trace("Binding parameter #{} as a character stream", valueBindDescriptor.getIndex());
                        binder.setCharacterStream(valueBindDescriptor.getIndex(), new StringReader((String) valueBindDescriptor.getValue()));
                    }
                    case Types.NCLOB, Types.LONGNVARCHAR -> {
                        LOGGER.trace("Binding parameter #{} as a national character stream", valueBindDescriptor.getIndex());
                        binder.setNCharacterStream(valueBindDescriptor.getIndex(), new StringReader((String) valueBindDescriptor.getValue()));
                    }
                    case Types.BLOB -> {
                        LOGGER.trace("Binding parameter #{} as BLOB", valueBindDescriptor.getIndex());
                        final Blob blob = binder.getConnection().createBlob();
                        blob.setBytes(1, (byte[]) valueBindDescriptor.getValue());
                        binder.setBlob(valueBindDescriptor.getIndex(), blob);
                    }
                    default -> {
                        LOGGER.trace("Binding parameter #{} as Jdbc Type {}", valueBindDescriptor.getIndex(), valueBindDescriptor.getTargetSqlType());
                        binder.setObject(valueBindDescriptor.getIndex(), valueBindDescriptor.getValue(), valueBindDescriptor.getTargetSqlType());
                    }
                }
            }
            else {
                LOGGER.trace("Binding parameter #{} as Jdbc Object", valueBindDescriptor.getIndex());
                binder.setObject(valueBindDescriptor.getIndex(), valueBindDescriptor.getValue());
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
