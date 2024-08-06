/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.junit.jupiter;

/**
 * An enumeration of all the possible sink database types supported in the test pipeline matrix.
 *
 * @author Chris Cranford
 */
public enum SinkType {

    MYSQL("mysql"),
    POSTGRES("postgres"),
    SQLSERVER("sqlserver"),
    ORACLE("oracle"),
    DB2("db2");

    private final String value;

    SinkType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public boolean is(SinkType... types) {
        for (SinkType type : types) {
            if (this.equals(type)) {
                return true;
            }
        }
        return false;
    }

    public static SinkType parse(String value) {
        for (SinkType type : SinkType.values()) {
            if (type.getValue().equalsIgnoreCase(value)) {
                return type;
            }
        }
        throw new IllegalStateException("Unknown source type: " + value);
    }

}
