/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.relational;

import io.debezium.common.annotation.Incubating;
import io.debezium.relational.Column;

/**
 * A {@link Column} of an Oracle table, the type of every column the Oracle connector creates through
 * {@link OracleRelationalMetadataFactory}. Oracle-specific column metadata is added here.
 *
 * @author Chris Cranford
 */
@Incubating
public interface OracleColumn extends Column {

    /**
     * Get the maximum length of the column's values in bytes, which Oracle reports as {@code DATA_LENGTH}.
     * <p>
     * For character types this differs from {@link #length()} when the column uses {@code CHAR} length
     * semantics or a national character type, because {@link #length()} is then the length in characters.
     * The value is set for {@code VARCHAR2} and {@code NVARCHAR2} columns.
     *
     * @return the length in bytes, or {@link Column#UNSET_INT_VALUE} when it is not known, for example
     *         for a column restored from schema history written by an older connector version
     */
    int octetLength();

    @Override
    OracleColumnEditor edit();
}
