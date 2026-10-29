/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.relational;

import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditorImpl;
import io.debezium.util.Interner;

/**
 * The {@link OracleColumnEditor} implementation.
 *
 * @author Chris Cranford
 */
final class OracleColumnEditorImpl extends ColumnEditorImpl implements OracleColumnEditor {

    private int octetLength = Column.UNSET_INT_VALUE;

    @Override
    public int octetLength() {
        return octetLength;
    }

    @Override
    public OracleColumnEditor octetLength(int octetLength) {
        this.octetLength = octetLength;
        return this;
    }

    @Override
    public OracleColumn create() {
        return Interner.intern(new OracleColumnImpl(this));
    }
}
