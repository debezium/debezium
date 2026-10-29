/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.relational;

import java.util.Objects;

import io.debezium.relational.ColumnImpl;

/**
 * The {@link OracleColumn} implementation.
 *
 * @author Chris Cranford
 */
final class OracleColumnImpl extends ColumnImpl implements OracleColumn {

    private final int octetLength;

    OracleColumnImpl(OracleColumnEditor editor) {
        super(editor);
        this.octetLength = editor.octetLength();
    }

    @Override
    public int octetLength() {
        return octetLength;
    }

    @Override
    public OracleColumnEditor edit() {
        return copyTo(new OracleColumnEditorImpl()).octetLength(octetLength);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof OracleColumn that && super.equals(that) && octetLength == that.octetLength();
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), octetLength);
    }

    @Override
    protected void appendExtensions(StringBuilder sb) {
        if (octetLength != UNSET_INT_VALUE) {
            sb.append(" OCTET_LENGTH ").append(octetLength);
        }
    }
}
