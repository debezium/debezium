/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.valueholder;

/**
 * This class is a wrapper class which holds LogMinerColumnValue
 * and the indicator if the column was processed by a parser listener.
 * The "processed" is "true" means a listener has parsed a value.
 * The "false" value means the this value was not parsed yet
 * The "processed" flag helps to filter the resulting collection of "new" and "old" values.
 *
 */
public class LogMinerColumnValueWrapper {

    private boolean processed;
    private final LogMinerColumnValue columnValue;

    public LogMinerColumnValueWrapper(LogMinerColumnValue columnValue) {
        this.columnValue = columnValue;
    }

    public LogMinerColumnValue getColumnValue() {
        return columnValue;
    }

    public boolean isProcessed() {
        return processed;
    }

    public void setProcessed(boolean processed) {
        this.processed = processed;
    }

}
