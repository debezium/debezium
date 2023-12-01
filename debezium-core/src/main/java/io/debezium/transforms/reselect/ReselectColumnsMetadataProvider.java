/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.reselect;

import java.util.List;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;

import io.debezium.relational.TableId;

/**
 * @author Chris Cranford
 */
public interface ReselectColumnsMetadataProvider<R extends ConnectRecord<R>> {
    /**
     * @return the query provider name
     */
    String getName();

    /**
     * Get the query to be used to reselect the columns.
     *
     * @param record the current record being processed, should not be {@code null}
     * @param source the source information block, should not be {@code null}
     * @param columns the list of columns to re-select, should not be empty
     * @param tableId the table id the re-select is based upon, should not be {@code null}
     * @return the re-select query to be used
     */
    String getQuery(R record, Struct source, List<String> columns, TableId tableId);

    /**
     * Converts a column's value
     *
     * @param jdbcType the column's JDBC type
     * @param value the selected value from the JDBC result set
     * @return converted value
     */
    Object convertValue(int jdbcType, Object value);

}
