/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.relational;

import io.debezium.relational.ColumnImpl;

/**
 * The {@link OracleColumn} implementation.
 *
 * @author Chris Cranford
 */
final class OracleColumnImpl extends ColumnImpl implements OracleColumn {

    OracleColumnImpl(OracleColumnEditor editor) {
        super(editor);
    }

    @Override
    public OracleColumnEditor edit() {
        return copyTo(new OracleColumnEditorImpl());
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof OracleColumn && super.equals(obj);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
