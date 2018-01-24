/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.postgresql.core.Oid;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.util.PSQLException;

import io.debezium.connector.postgresql.connection.ReplicationMessage;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.TableId;

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
    /**
     * Types defined by the PostGIS extension
     * OIDs are dynamic... so we alias them to static integers outside the OID range.
     */
    public static final int POSTGIS_GEOMETRY = -101;
    public static final int POSTGIS_GEOGRAPHY = -102;
    public static final int POSTGIS_GEOMETRY_ARRAY = -201;
    public static final int POSTGIS_GEOGRAPHY_ARRAY = -202;

    private PgOid() {
    }

    protected static int jdbcColumnToOid(Column column) {
        String typeName = TableId.parse(column.typeName()).table().toUpperCase();

        if (column.jdbcType() == Types.ARRAY) {
            // until array handling is cleaner, geometry arrays aren't supported
            // if (typeName.equals("_GEOMETRY")) {
            //     return PgOid.POSTGIS_GEOMETRY;
            // } else if (typeName.equals("_GEOGRAPHY")) {
            //     return PgOid.POSTGIS_GEOGRAPHY;
            // }

            return column.typeName() != null ? typeNameToOid(column.typeName().substring(1) + "_array")
                    : column.componentType();
        }
        return typeNameToOid(typeName);
    }

    public static int typeNameToOid(String typeName) {
        switch (typeName.toUpperCase()) {
        case "TSTZRANGE":
            return TSTZRANGE_OID;
        case "SMALLSERIAL":
            return PgOid.INT2;
        case "SERIAL":
            return PgOid.INT4;
        case "BIGSERIAL":
            return PgOid.INT8;
        case "JSONB":
            return PgOid.JSONB_OID;
        case "GEOMETRY":
            return PgOid.POSTGIS_GEOMETRY;
        case "GEOGRAPHY":
            return PgOid.POSTGIS_GEOGRAPHY;
        // until array handling is cleaner, geometry arrays aren't supported
        // case "_GEOMETRY":
        //     return PgOid.POSTGIS_GEOMETRY_ARRAY;
        // case "_GEOGRAPHY":
        //     return PgOid.POSTGIS_GEOGRAPHY_ARRAY;
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

    /**
     * Returns an unqualified type name (ie. no schema) for an OID
     * @throws SQLException
     */
    public static String oidToTypeName(PgConnection conn, int oid) throws SQLException {
        String typeName = conn.getTypeInfo().getPGType(oid);
        // column types here can be '"schema"."type"' too...
        return TableId.parse(typeName).table().toUpperCase();
    }

}
