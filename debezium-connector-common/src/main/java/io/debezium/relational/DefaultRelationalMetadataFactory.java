/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import io.debezium.common.annotation.Incubating;

/**
 * The {@link RelationalMetadataFactory} behind {@link Column#editor()} and {@link Table#editor()}.
 * It holds no state, so the single {@link #INSTANCE} serves every connector that does not override
 * the factory.
 *
 * @author Chris Cranford
 */
@Incubating
public final class DefaultRelationalMetadataFactory implements RelationalMetadataFactory {

    public static final DefaultRelationalMetadataFactory INSTANCE = new DefaultRelationalMetadataFactory();

    private DefaultRelationalMetadataFactory() {
    }

    @Override
    public ColumnEditorImpl columnEditor() {
        return new ColumnEditorImpl();
    }

    @Override
    public TableEditorImpl tableEditor() {
        return new TableEditorImpl();
    }
}
