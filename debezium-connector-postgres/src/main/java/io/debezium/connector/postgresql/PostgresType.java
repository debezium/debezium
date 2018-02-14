/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.util.Objects;

import org.postgresql.core.Oid;
import org.postgresql.core.TypeInfo;

/**
 * A class that binds together a PostgresSQL OID, JDBC type id and the string name of the type.
 * The array types contain link to their element type.
 *
 * @author Jiri Pechanec
 *
 */
public class PostgresType {

    public static final PostgresType UNKNOWN = new PostgresType("unknown", -1, Integer.MIN_VALUE, null);

    private final String name;
    private final int oid;
    private final int jdbcId;
    private final PostgresType elementType;
    private final TypeInfo typeInfo;

    public PostgresType(String name, int oid, int jdbcId, TypeInfo typeInfo) {
        this(name, oid, jdbcId, typeInfo, null);
    }

    public PostgresType(String name, int oid, int jdbcId, TypeInfo typeInfo, PostgresType elementType) {
        Objects.requireNonNull(name);
        this.name = name;
        this.oid = oid;
        this.jdbcId = jdbcId;
        this.elementType = elementType;
        this.typeInfo = typeInfo;
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
       if (typeInfo == null) {
           return TypeRegistry.UNKNOWN_LENGTH;
       }
       int size = typeInfo.getPrecision(oid, TypeRegistry.NO_TYPE_MODIFIER);
       if (size == 0) {
           size = typeInfo.getDisplaySize(oid, TypeRegistry.NO_TYPE_MODIFIER);
       }
       return size;
   }

    /**
     *
     * @return the default scale of the type
     */
    public int getDefaultScale() {
        if (typeInfo == null) {
            return TypeRegistry.UNKNOWN_LENGTH;
        }
        return typeInfo.getScale(oid, TypeRegistry.NO_TYPE_MODIFIER);
    }

    /**
     * @param modifier - type modifier coming from decoder
     * @return length of the type based on the modifier
     */
    public int length(int modifier) {
        if (typeInfo == null) {
            return TypeRegistry.UNKNOWN_LENGTH;
        }
        switch (oid) {
        case Oid.TIMESTAMP:
        case Oid.TIMESTAMPTZ:
        case Oid.TIME:
        case Oid.TIMETZ:
        case Oid.INTERVAL:
            return typeInfo.getPrecision(oid, modifier);
        }
        return modifier;
    }

    /**
     * @param modifier - type modifier coming from decoder
     * @return scale of the type based on the modifier
     */
    public int scale(int modifier) {
        if (typeInfo == null) {
            return TypeRegistry.UNKNOWN_LENGTH;
        }
        switch (oid) {
        case Oid.TIMESTAMP:
        case Oid.TIMESTAMPTZ:
        case Oid.TIME:
        case Oid.TIMETZ:
        case Oid.INTERVAL:
            return typeInfo.getScale(oid, modifier);
        }
        return getDefaultScale();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + oid;
        return result;
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
        return "PostgresType [name=" + name + ", oid=" + oid + ", jdbcId=" + jdbcId + ", defaultLength=" + getDefaultLength()
                + ", defaultScale=" + getDefaultScale() + ", elementType=" + elementType + "]";
    }
}
