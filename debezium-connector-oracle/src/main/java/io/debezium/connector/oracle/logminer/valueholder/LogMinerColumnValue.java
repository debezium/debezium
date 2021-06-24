/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.valueholder;

/**
 * @deprecated This has been deprecated and should no longer be used.
 * This will be removed in conjunction with {@link io.debezium.connector.oracle.logminer.parser.SimpleDmlParser}.
 */
@Deprecated
public interface LogMinerColumnValue {

    /**
     * @return value of the database record
     * with exception of LOB types
     */
    Object getColumnData();

    /**
     * @return column name
     */
    String getColumnName();

    /**
     * This sets the database record value with the exception of LOBs
     * @param columnData data
     */
    void setColumnData(Object columnData);
}
