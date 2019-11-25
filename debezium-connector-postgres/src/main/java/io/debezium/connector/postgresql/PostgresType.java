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

    public static final PostgresType UNKNOWN = new PostgresType("unknown", -1, Integer.MIN_VALUE, null, null, null);

    private final String name;
    private final int oid;
    private final int jdbcId;
    private final PostgresType baseType;
    private final PostgresType elementType;
    private final TypeInfo typeInfo;
    private final int modifiers;

    private PostgresType(String name, int oid, int jdbcId, TypeInfo typeInfo, PostgresType baseType, PostgresType elementType) {
        this(name, oid, jdbcId, TypeRegistry.NO_TYPE_MODIFIER, typeInfo, baseType, elementType);
    }

    private PostgresType(String name, int oid, int jdbcId, int modifiers, TypeInfo typeInfo, PostgresType baseType, PostgresType elementType) {
        Objects.requireNonNull(name);
        this.name = name;
        this.oid = oid;
        this.jdbcId = jdbcId;
        this.typeInfo = typeInfo;
        this.baseType = baseType;
        this.elementType = elementType;
        this.modifiers = modifiers;
    }

    /**
     * @return true if this type is an array
     */
    public boolean isArrayType() {
        return elementType != null;
    }

    /**
     * @return true if this type is a base type
     */
    public boolean isBaseType() {
        return baseType == null;
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
     * @return the base postgres type this type is based upon
     */
    public PostgresType getBaseType() {
        return baseType;
    }

    /**
     *
     * @return the default length of the type
     */
    public int getDefaultLength() {
        if (typeInfo == null) {
            return TypeRegistry.UNKNOWN_LENGTH;
        }
        if (baseType != null) {
            if (modifiers == TypeRegistry.NO_TYPE_MODIFIER) {
                return baseType.getDefaultLength();
            }
            else {
                int size = typeInfo.getPrecision(baseType.getOid(), modifiers);
                if (size == 0) {
                    size = typeInfo.getDisplaySize(baseType.getOid(), modifiers);
                }
                if (size != 0 && size != Integer.MAX_VALUE) {
                    return size;
                }
            }
        }
        int size = typeInfo.getPrecision(oid, modifiers);
        if (size == 0) {
            size = typeInfo.getDisplaySize(oid, modifiers);
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
        if (baseType != null) {
            if (modifiers == TypeRegistry.NO_TYPE_MODIFIER) {
                return baseType.getDefaultScale();
            }
            else {
                return typeInfo.getScale(baseType.getOid(), modifiers);
            }
        }
        return typeInfo.getScale(oid, modifiers);
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
     * Get the underlying postgres type information object
     * @return the type information object; may be null
     */
    public TypeInfo getTypeInfo() {
        return typeInfo;
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
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        PostgresType other = (PostgresType) obj;
        if (oid != other.oid) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "PostgresType [name=" + name + ", oid=" + oid + ", jdbcId=" + jdbcId + ", modifiers=" + modifiers + ", defaultLength=" + getDefaultLength()
                + ", defaultScale=" + getDefaultScale() + ", baseType=" + baseType + ", elementType=" + elementType + "]";
    }

    public static class Builder {
        private final TypeRegistry typeRegistry;
        private final String name;
        private final int oid;
        private final int jdbcId;
        private final int modifiers;
        private final TypeInfo typeInfo;
        private int baseTypeOid;
        private int elementTypeOid;

        public Builder(TypeRegistry typeRegistry, String name, int oid, int jdbcId, int modifiers, TypeInfo typeInfo) {
            this.typeRegistry = typeRegistry;
            this.name = name;
            this.oid = oid;
            this.jdbcId = jdbcId;
            this.modifiers = modifiers;
            this.typeInfo = typeInfo;
        }

        public Builder baseType(int baseTypeOid) {
            this.baseTypeOid = baseTypeOid;
            return this;
        }

        public Builder elementType(int elementTypeOid) {
            this.elementTypeOid = elementTypeOid;
            return this;
        }

        public PostgresType build() {
            PostgresType baseType = null;
            if (baseTypeOid != 0) {
                baseType = typeRegistry.get(baseTypeOid);
            }

            PostgresType elementType = null;
            if (elementTypeOid != 0) {
                elementType = typeRegistry.get(elementTypeOid);
            }

            return new PostgresType(name, oid, jdbcId, modifiers, typeInfo, baseType, elementType);
        }
    }
}
