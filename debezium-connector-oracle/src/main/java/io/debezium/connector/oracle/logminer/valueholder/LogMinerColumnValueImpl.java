/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.valueholder;

import java.util.Objects;

import io.debezium.connector.oracle.antlr.listener.ParserUtils;

/**
 * This class stores parsed column info
 *
 * @deprecated This has been deprecated and should no longer be used.
 * This will be removed in conjunction with {@link io.debezium.connector.oracle.logminer.parser.SimpleDmlParser}.
 */
@Deprecated
public class LogMinerColumnValueImpl implements LogMinerColumnValue {

    private String columnName;
    private Object columnData;

    public LogMinerColumnValueImpl(String columnName) {
        this.columnName = columnName;
    }

    @Override
    public Object getColumnData() {
        return columnData;
    }

    @Override
    public String getColumnName() {
        return columnName;
    }

    @Override
    public void setColumnData(Object columnData) {
        if (columnData instanceof String) {
            this.columnData = ParserUtils.replaceDoubleBackSlashes((String) columnData);
        }
        else {
            this.columnData = columnData;
        }
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogMinerColumnValueImpl that = (LogMinerColumnValueImpl) o;
        return Objects.equals(columnName, that.columnName) &&
                Objects.equals(columnData, that.columnData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnName, columnData);
    }

    @Override
    public String toString() {
        return "LogMinerColumnValueImpl{columnName=" + columnName + ", columnData=" + columnData + "}";
    }
}
