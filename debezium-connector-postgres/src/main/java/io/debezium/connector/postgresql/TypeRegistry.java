/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.postgresql.core.TypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    public static final String TYPE_NAME_CITEXT = "citext";
    public static final String TYPE_NAME_HSTORE = "hstore";
    public static final String TYPE_NAME_LTREE = "ltree";

    public static final String TYPE_NAME_HSTORE_ARRAY = "_hstore";
    public static final String TYPE_NAME_GEOGRAPHY_ARRAY = "_geography";
    public static final String TYPE_NAME_GEOMETRY_ARRAY = "_geometry";
    public static final String TYPE_NAME_CITEXT_ARRAY = "_citext";
    public static final String TYPE_NAME_LTREE_ARRAY = "_ltree";

    public static final int NO_TYPE_MODIFIER = -1;
    public static final int UNKNOWN_LENGTH = -1;

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
     * Builder for instances of {@link TypeRegistry}.
     */
    public static final class Builder {

        private final Map<String, PostgresType> nameToType = new HashMap<>();
        private final Map<Integer, PostgresType> oidToType = new HashMap<>();

        private int geometryOid = Integer.MIN_VALUE;
        private int geographyOid = Integer.MIN_VALUE;
        private int citextOid = Integer.MIN_VALUE;
        private int hstoreOid = Integer.MIN_VALUE;
        private int ltreeOid = Integer.MIN_VALUE;

        private int hstoreArrayOid = Integer.MIN_VALUE;
        private int geometryArrayOid = Integer.MIN_VALUE;
        private int geographyArrayOid = Integer.MIN_VALUE;
        private int citextArrayOid = Integer.MIN_VALUE;
        private int ltreeArrayOid = Integer.MIN_VALUE;

        private Builder() {
        }

        /**
         * Add a new type
         *
         * @param type
         *
         * @return builder instance
         */
        public Builder addType(PostgresType type) {
            oidToType.put(type.getOid(), type);
            nameToType.put(type.getName(), type);

            if (TYPE_NAME_GEOMETRY.equals(type.getName())) {
                geometryOid = type.getOid();
            }
            else if (TYPE_NAME_GEOGRAPHY.equals(type.getName())) {
                geographyOid = type.getOid();
            }
            else if (TYPE_NAME_CITEXT.equals(type.getName())) {
                citextOid = type.getOid();
            }
            else if (TYPE_NAME_HSTORE.equals(type.getName())) {
                hstoreOid = type.getOid();
            }
            else if (TYPE_NAME_LTREE.equals(type.getName())) {
                ltreeOid = type.getOid();
            }
            else if (TYPE_NAME_HSTORE_ARRAY.equals(type.getName())) {
                hstoreArrayOid = type.getOid();
            }
            else if (TYPE_NAME_GEOMETRY_ARRAY.equals(type.getName())) {
                geometryArrayOid = type.getOid();
            }
            else if (TYPE_NAME_GEOGRAPHY_ARRAY.equals(type.getName())) {
                geographyArrayOid = type.getOid();
            }
            else if (TYPE_NAME_CITEXT_ARRAY.equals(type.getName())) {
                citextArrayOid = type.getOid();
            }
            else if (TYPE_NAME_LTREE_ARRAY.equals(type.getName())) {
                ltreeArrayOid = type.getOid();
            }
            return this;
        }

        /**
         *
         * @param oid - PostgreSQL OID
         * @return type associated with the given OID
         */
        public PostgresType get(int oid) {
            return oidToType.get(oid);
        }

        /**
         * @return initialized type registry
         */
        public TypeRegistry build() {
            return new TypeRegistry(this);
        }
    }

    public static Builder create(TypeInfo typeInfo) {
        return new Builder();
    }

    private final Map<String, PostgresType> nameToType;
    private final Map<Integer, PostgresType> oidToType;

    private final int geometryOid;
    private final int geographyOid;
    private final int citextOid;
    private final int hstoreOid;
    private final int ltreeOid;

    private final int hstoreArrayOid;
    private final int geometryArrayOid;
    private final int geographyArrayOid;
    private final int citextArrayOid;
    private final int ltreeArrayOid;

    private TypeRegistry(Builder builder) {

        this.nameToType = Collections.unmodifiableMap(builder.nameToType);
        this.oidToType = Collections.unmodifiableMap(builder.oidToType);

        this.geometryOid = builder.geometryOid;
        this.geographyOid = builder.geographyOid;
        this.citextOid = builder.citextOid;
        this.hstoreOid = builder.hstoreOid;
        this.ltreeOid = builder.ltreeOid;

        this.hstoreArrayOid = builder.hstoreArrayOid;
        this.geometryArrayOid = builder.geometryArrayOid;
        this.geographyArrayOid = builder.geographyArrayOid;
        this.citextArrayOid = builder.citextArrayOid;
        this.ltreeArrayOid = builder.ltreeArrayOid;

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
     * @return OID for {@code CITEXT} type of this PostgreSQL instance
     */
    public int citextOid() {
        return citextOid;
    }

    /**
     *
     * @return OID for {@code HSTORE} type of this PostgreSQL instance
     */
    public int hstoreOid() {
        return hstoreOid;
    }

    /**
     *
     * @return OID for {@code LTREE} type of this PostgreSQL instance
     */
    public int ltreeOid() {
        return ltreeOid;
    }

    /**
    *
    * @return OID for array of {@code HSTORE} type of this PostgreSQL instance
    */
    public int hstoreArrayOid() {
        return hstoreArrayOid;
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
     *
     * @return OID for array of {@code CITEXT} type of this PostgreSQL instance
     */
    public int citextArrayOid() {
        return citextArrayOid;
    }

    /**
     *
     * @return OID for array of {@code LTREE} type of this PostgreSQL instance
     */
    public int ltreeArrayOid() {
        return ltreeArrayOid;
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
}
