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

    @Override
    OracleColumnEditor edit();
}
