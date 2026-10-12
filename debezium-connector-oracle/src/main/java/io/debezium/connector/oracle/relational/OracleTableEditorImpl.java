/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.relational;

import io.debezium.relational.TableEditorImpl;
import io.debezium.util.Interner;

/**
 * The {@link OracleTableEditor} implementation.
 *
 * @author Chris Cranford
 */
final class OracleTableEditorImpl extends TableEditorImpl implements OracleTableEditor {

    @Override
    public OracleTable create() {
        checkCreatable();
        return Interner.intern(new OracleTableImpl(this));
    }
}
