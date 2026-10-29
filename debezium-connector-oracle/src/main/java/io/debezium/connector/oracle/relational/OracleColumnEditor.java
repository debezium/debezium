/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.relational;

import io.debezium.common.annotation.Incubating;
import io.debezium.relational.ColumnEditor;

/**
 * An editor for {@link OracleColumn} instances.
 *
 * @author Chris Cranford
 */
@Incubating
public interface OracleColumnEditor extends ColumnEditor {

    /**
     * Get the maximum length of the column's values in bytes.
     *
     * @return the length in bytes, or {@link io.debezium.relational.Column#UNSET_INT_VALUE} when not known
     */
    int octetLength();

    /**
     * Set the maximum length of the column's values in bytes.
     *
     * @param octetLength the length in bytes, or {@link io.debezium.relational.Column#UNSET_INT_VALUE} when not known
     * @return this editor so methods can be chained together; never null
     */
    OracleColumnEditor octetLength(int octetLength);

    @Override
    OracleColumn create();
}
