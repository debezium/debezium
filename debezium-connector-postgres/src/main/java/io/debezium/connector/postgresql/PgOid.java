/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.postgresql.core.Oid;
import org.postgresql.util.PSQLException;

import io.debezium.connector.postgresql.connection.ReplicationMessage;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;

/**
 * Extension to the {@link org.postgresql.core.Oid} class which contains Postgres specific datatypes not found currently in the
 * JDBC driver implementation classes.
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public final class PgOid extends Oid {

    /**
     * A list of PG types not known by the JDBC driver atm.
     */
    public static final int JSONB_JDBC_OID = 1111;

    /**
     * Internal PG types as returned by the plugin
     */
    public static final int JSONB_OID = 3802;

    public static final int TSTZRANGE_OID = 3910;

    private static final Map<String, String> LONG_TYPE_NAMES = new HashMap<>();
    static {
        LONG_TYPE_NAMES.put("bigint", "int8");
        LONG_TYPE_NAMES.put("bit varying", "varbit");
        LONG_TYPE_NAMES.put("boolean", "bool");
        LONG_TYPE_NAMES.put("character", "bpchar");
        LONG_TYPE_NAMES.put("character varying", "varchar");
        LONG_TYPE_NAMES.put("double precision", "float8");
        LONG_TYPE_NAMES.put("integer", "int4");
        LONG_TYPE_NAMES.put("real", "float4");
        LONG_TYPE_NAMES.put("smallint", "int2");
        LONG_TYPE_NAMES.put("timestamp without time zone", "timestamp");
        LONG_TYPE_NAMES.put("timestamp with time zone", "timestamptz");
        LONG_TYPE_NAMES.put("time without time zone", "time");
        LONG_TYPE_NAMES.put("time with time zone", "timetz");
    }

    private PgOid() {
    }

    protected static int jdbcColumnToOid(Column column) {
        String typeName = column.typeName();

        if (typeName.toUpperCase().equals("TSTZRANGE")) {
            return TSTZRANGE_OID;
        } else if (typeName.toUpperCase().equals("SMALLSERIAL")) {
            return PgOid.INT2;
        } else if (typeName.toUpperCase().equals("SERIAL")) {
            return PgOid.INT4;
        } else if (typeName.toUpperCase().equals("BIGSERIAL")) {
            return PgOid.INT8;
        } else if (typeName.toUpperCase().equals("JSONB")) {
            return PgOid.JSONB_OID;
        } else if (column.jdbcType() == Types.ARRAY) {
            return column.componentType();
        }
        try {
            return Oid.valueOf(typeName);
        } catch (PSQLException e) {
            // not known by the driver PG driver
            return Oid.UNSPECIFIED;
        }
    }

    /**
     * Converts a type name in long (readable) format like <code>boolean</code> to s standard
     * data type name like <code>bool</code>.
     * 
     * @param typeName - a type name in long format 
     * @return - the type name in standardized format
     */
    public static String normalizeTypeName(String typeName) {
        return LONG_TYPE_NAMES.getOrDefault(typeName, typeName);
    }

    /**
     * JDBC metadata are different for some of the unbounded types from those coming via decoder.
     * This method sets the type constraints to the values provided by JDBC metadata. 
     * 
     * @param column - a column coming from decoder
     * @param columnEditor - the JDBC counterpart of the column
     */
    public static void reconcileJdbcOidTypeConstraints(final ReplicationMessage.Column column,
            final ColumnEditor columnEditor) {
        switch (column.getTypeName()) {
            case "money":
                // JDBC returns scale 0 but decoder plugin returns -1 (unscaled)
                columnEditor.scale(0);
                break;
            case "timestamp":
                // JDBC returns length/scale 29/6 but decoder plugin returns -1 (unlimited)
                columnEditor.length(29);
                columnEditor.scale(6);
                break;
            case "time":
                // JDBC returns length/scale 15/6 but decoder plugin returns -1 (unlimited)
                columnEditor.length(15);
                columnEditor.scale(6);
                break;
        }
    }

}
