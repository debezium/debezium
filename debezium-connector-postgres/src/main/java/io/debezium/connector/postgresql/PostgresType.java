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

    public static final PostgresType UNKNOWN = new PostgresType("unknown", -1, Integer.MIN_VALUE); 

    private final String name;
    private final int oid;
    private final int jdbc;
    private final PostgresType element;

    public PostgresType(String name, int oid, int jdbc, PostgresType element) {
        Objects.requireNonNull(name);
        this.name = name;
        this.oid = oid;
        this.jdbc = jdbc;
        this.element = element;
    }

    public PostgresType(String name, int oid, int jdbc) {
        this(name, oid, jdbc, null);
    }

    /**
     * @return true if this type is an array
     */
    public boolean isArrayType() {
        return element != null;
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
        return jdbc;
    }

    /**
     *
     * @return the type of element in arrays or null for primitive types 
     */
    public PostgresType getElementType() {
        return element;
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
        if (element == null) {
            if (other.element != null)
                return false;
        }
        else if (!element.equals(other.element))
            return false;
        if (jdbc != other.jdbc)
            return false;
        if (!name.equals(other.name))
            return false;
        if (oid != other.oid)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "PostgresType [name=" + name + ", oid=" + oid + ", jdbc=" + jdbc + ", element=" + element + "]";
    }
}
