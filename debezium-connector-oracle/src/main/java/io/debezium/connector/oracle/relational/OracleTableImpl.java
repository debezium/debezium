/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.relational;

import io.debezium.relational.TableImpl;

/**
 * The {@link OracleTable} implementation.
 *
 * @author Chris Cranford
 */
final class OracleTableImpl extends TableImpl implements OracleTable {

    OracleTableImpl(OracleTableEditor editor) {
        super(editor);
    }

    @Override
    public OracleTableEditor edit() {
        return copyTo(new OracleTableEditorImpl());
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof OracleTable && super.equals(obj);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
