/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.relational;

import io.debezium.common.annotation.Incubating;
import io.debezium.relational.Table;

/**
 * A {@link Table} of an Oracle database, the type of every table the Oracle connector creates through
 * {@link OracleRelationalMetadataFactory}. Oracle-specific table metadata is added here.
 *
 * @author Chris Cranford
 */
@Incubating
public interface OracleTable extends Table {

    @Override
    OracleTableEditor edit();
}
