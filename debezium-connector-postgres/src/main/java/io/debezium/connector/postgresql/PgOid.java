/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.sql.Types;
import java.util.Collections;
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

    private static final Map<String, String> LONG_TYPE_NAMES = Collections.unmodifiableMap(getLongTypeNames());

    private static Map<String, String> getLongTypeNames() {
        Map<String, String> longTypeNames = new HashMap<>();

        longTypeNames.put("bigint", "int8");
        longTypeNames.put("bit varying", "varbit");
        longTypeNames.put("boolean", "bool");
        longTypeNames.put("character", "bpchar");
        longTypeNames.put("character varying", "varchar");
        longTypeNames.put("double precision", "float8");
        longTypeNames.put("integer", "int4");
        longTypeNames.put("real", "float4");
        longTypeNames.put("smallint", "int2");
        longTypeNames.put("timestamp without time zone", "timestamp");
        longTypeNames.put("timestamp with time zone", "timestamptz");
        longTypeNames.put("time without time zone", "time");
        longTypeNames.put("time with time zone", "timetz");

        return longTypeNames;
    }

    private PgOid() {
    }

    protected static int jdbcColumnToOid(Column column) {
        if (column.jdbcType() == Types.ARRAY) {
            return column.componentType();
        }
        return typeNameToOid(column.typeName());
    }

    public static int typeNameToOid(String typeName) {
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
     * @param columnTypeMetadata column type metadata coming from decoder
     * @param columnEditor the JDBC counterpart of the column
     */
    public static void reconcileJdbcOidTypeConstraints(ReplicationMessage.ColumnTypeMetadata columnTypeMetadata,
            final ColumnEditor columnEditor) {
        switch (columnTypeMetadata.getName()) {
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
