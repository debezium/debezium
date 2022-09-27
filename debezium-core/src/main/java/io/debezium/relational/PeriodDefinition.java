/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

public class PeriodDefinition {
    private String rowStartColumn;
    private String rowEndColumn;

    public PeriodDefinition(String rowStartColumn, String rowEndColumn) {
        this.rowStartColumn = rowStartColumn;
        this.rowEndColumn = rowEndColumn;
    }

    public String getRowStartColumn() {
        return rowStartColumn;
    }

    public void setRowStartColumn(String rowStartColumn) {
        this.rowStartColumn = rowStartColumn;
    }

    public String getRowEndColumn() {
        return rowEndColumn;
    }

    public void setRowEndColumn(String rowEndColumn) {
        this.rowEndColumn = rowEndColumn;
    }

    @Override
    public String toString() {
        return "PeriodDefinition{" +
                "rowStartColumn='" + rowStartColumn + '\'' +
                ", rowEndColumn='" + rowEndColumn + '\'' +
                '}';
    }
}
