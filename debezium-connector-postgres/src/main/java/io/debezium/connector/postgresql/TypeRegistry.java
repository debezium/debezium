/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.relational.ColumnEditor;

/**
 * A registry of types supported by a PostgreSQL instance. Allows lookup of the types according to
 * type name or OID.
 *
 * @author Jiri Pechanec
 *
 */
public class TypeRegistry {
    private static final Logger LOGGER = LoggerFactory.getLogger(TypeRegistry.class);

    public static final String TYPE_NAME_GEOGRAPHY = "geography";
    public static final String TYPE_NAME_GEOMETRY = "geometry";
    public static final String TYPE_NAME_GEOGRAPHY_ARRAY = "_geography";
    public static final String TYPE_NAME_GEOMETRY_ARRAY = "_geometry";

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

    public static interface Builder {
        Builder addType(PostgresType type);
        TypeRegistry build();
        PostgresType get(int oid);
    }

    public static Builder create() {
        return new Builder() {
            private final TypeRegistry registry = new TypeRegistry();

            /**
             * @return initialized type registry
             */
            @Override
            public TypeRegistry build() {
                return registry;
            }

            /**
             * Add a new type
             *
             * @param type
             *
             * @return builder instance
             */
            @Override
            public Builder addType(PostgresType type) {
                registry.register(type);
                return this;
            }

            /**
             *
             * @param oid - PostgreSQL OID
             * @return type associated with the given OID
             */
            @Override
            public PostgresType get(int oid) {
                return registry.get(oid);
            }
        };
    }

    private final Map<String, PostgresType> nameToType = new HashMap<>();
    private final Map<Integer, PostgresType> oidToType = new HashMap<>();
    private int geometryOid = Integer.MIN_VALUE;
    private int geographyOid = Integer.MIN_VALUE;
    private int geometryArrayOid = Integer.MIN_VALUE;
    private int geographyArrayOid = Integer.MIN_VALUE;

    private TypeRegistry() {
    }

    /**
     * Register a type to the registry
     *
     * @param type
     */
    private void register(PostgresType type) {
        oidToType.put(type.getOid(), type);
        nameToType.put(type.getName(), type);
        if (TYPE_NAME_GEOMETRY.equals(type.getName())) {
            geometryOid = type.getOid();
        }
        else if (TYPE_NAME_GEOGRAPHY.equals(type.getName())) {
            geographyOid = type.getOid();
        }
        else if (TYPE_NAME_GEOMETRY_ARRAY.equals(type.getName())) {
            geometryArrayOid = type.getOid();
        }
        else if (TYPE_NAME_GEOGRAPHY_ARRAY.equals(type.getName())) {
            geographyArrayOid = type.getOid();
        }
    }

    /**
     *
     * @param oid - PostgreSQL OID
     * @return type associated with the given OID
     */
    public PostgresType get(int oid) {
        PostgresType r = oidToType.get(oid);
        if (r == null) {
            LOGGER.warn("Unknown OID {} requested", oid);
            r = PostgresType.UNKNOWN;
        }
        return r;
    }

    /**
     *
     * @param name - PostgreSQL type name
     * @return type associated with the given type name
     */
    public PostgresType get(String name) {
        switch (name) {
            case "serial":
                name = "int4";
                break;
            case "smallserial":
                name = "int2";
                break;
            case "bigserial":
                name = "int8";
                break;
        }
        String[] parts = name.split("\\.");
        if (parts.length > 1) {
            name = parts[1];
        }
        if (name.charAt(0) == '"') {
            name = name.substring(1, name.length() - 1);
        }
        PostgresType r = nameToType.get(name);
        if (r == null) {
            LOGGER.warn("Unknown type named {} requested", name);
            r = PostgresType.UNKNOWN;
        }
        return r;
    }

    /**
     *
     * @return OID for {@code GEOMETRY} type of this PostgreSQL instance
     */
    public int geometryOid() {
        return geometryOid;
    }

    /**
     *
     * @return OID for {@code GEOGRAPHY} type of this PostgreSQL instance
     */
    public int geographyOid() {
        return geographyOid;
    }

    /**
     *
     * @return OID for array of {@code GEOMETRY} type of this PostgreSQL instance
     */
    public int geometryArrayOid() {
        return geometryArrayOid;
    }

    /**
     *
     * @return OID for array of {@code GEOGRAPHY} type of this PostgreSQL instance
     */
    public int geographyArrayOid() {
        return geographyArrayOid;
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
     * @param type column type coming from decoder
     * @param columnEditor the JDBC counterpart of the column
     */
    public static void reconcileJdbcOidTypeConstraints(PostgresType type,
            final ColumnEditor columnEditor) {
        switch (type.getName()) {
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
