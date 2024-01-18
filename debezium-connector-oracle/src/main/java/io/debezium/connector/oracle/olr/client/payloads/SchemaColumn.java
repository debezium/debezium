/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.olr.client.payloads;

/**
 * Represents a column within a table's schema block.
 *
 * @author Chris Cranford
 */
public class SchemaColumn {

    private String name;
    private String type;
    private Integer precision;
    private Integer scale;
    private Integer length;
    private boolean nullable;

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public Integer getPrecision() {
        return precision;
    }

    public Integer getScale() {
        return scale;
    }

    public Integer getLength() {
        return length;
    }

    public boolean isNullable() {
        return nullable;
    }

    @Override
    public String toString() {
        return "SchemaColumn{" +
                "name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", precision=" + precision +
                ", scale=" + scale +
                ", length=" + length +
                ", nullable=" + nullable +
                '}';
    }

}
