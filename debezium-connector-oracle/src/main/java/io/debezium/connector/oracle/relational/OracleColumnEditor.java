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

    @Override
    OracleColumn create();
}
