/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.junit.jupiter.e2e.source;

/**
 * An enumeration of all the possible source database types supported in the test pipeline matrix.
 *
 * @author Chris Cranford
 */
public enum SourceType {

    MYSQL("mysql"),
    POSTGRES("postgres"),
    SQLSERVER("sqlserver"),
    ORACLE("oracle");

    // removed temporarily because there is quite a bit of setup required for DB2 in the test suite
    // DB2("db2");

    private final String value;

    SourceType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public boolean is(SourceType... types) {
        for (SourceType type : types) {
            if (type == this) {
                return true;
            }
        }
        return false;
    }

    public static SourceType parse(String value) {
        for (SourceType type : SourceType.values()) {
            if (type.getValue().equalsIgnoreCase(value)) {
                return type;
            }
        }
        throw new IllegalStateException("Unknown source type: " + value);
    }
}
