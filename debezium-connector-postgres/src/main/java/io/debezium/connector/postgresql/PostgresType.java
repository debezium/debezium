/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.util.Objects;

/**
 * A class that binds together a PostgresSQL OID, JDBC type id and the string name of the type.
 * The array types contain link to their element type.
 *
 * @author Jiri Pechanec
 *
 */
public class PostgresType {

    public static final PostgresType UNKNOWN = new PostgresType("unknown", -1, Integer.MIN_VALUE, TypeRegistry.UNKNOWN_LENGTH, TypeRegistry.UNKNOWN_LENGTH);

    private final String name;
    private final int oid;
    private final int jdbcId;
    private final PostgresType elementType;
    private final int defaultLength;
    private final int defaultScale;

    public PostgresType(String name, int oid, int jdbcId, int defaultLength, int defaultScale) {
        this(name, oid, jdbcId, defaultLength, defaultScale, null);
    }

    public PostgresType(String name, int oid, int jdbcId, int defaultLength, int defaultScale, PostgresType elementType) {
        Objects.requireNonNull(name);
        this.name = name;
        this.oid = oid;
        this.jdbcId = jdbcId;
        this.elementType = elementType;
        this.defaultLength = defaultLength;
        this.defaultScale = defaultScale;
    }

    /**
     * @return true if this type is an array
     */
    public boolean isArrayType() {
        return elementType != null;
    }

    /**
     *
     * @return symbolic name of the type
     */
    public String getName() {
        return name;
    }

    /**
     *
     * @return PostgreSQL OID of this type
     */
    public int getOid() {
        return oid;
    }

    /**
     *
     * @return JDBC id of the type as reported by JDBC metadata
     */
    public int getJdbcId() {
        return jdbcId;
    }

    /**
     *
     * @return the type of element in arrays or null for primitive types
     */
    public PostgresType getElementType() {
        return elementType;
    }

    /**
     *
     * @return the default length of the type
     */
   public int getDefaultLength() {
       return defaultLength;
   }

    /**
     *
     * @return the default Scale of the type
     */
    public int getDefaultScale() {
        return defaultScale;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        PostgresType other = (PostgresType) obj;
        if (oid != other.oid)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "PostgresType [name=" + name + ", oid=" + oid + ", jdbcId=" + jdbcId + ", elementType=" + elementType + "]";
    }
}
