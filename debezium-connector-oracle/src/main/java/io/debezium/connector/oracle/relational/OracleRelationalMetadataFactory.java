/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.relational;

import io.debezium.common.annotation.Incubating;
import io.debezium.relational.RelationalMetadataFactory;

/**
 * Creates the Oracle connector's {@link OracleColumn} and {@link OracleTable} instances. The default
 * serializers rebuild tables through this factory, so schema history recovery yields Oracle types too.
 *
 * @author Chris Cranford
 */
@Incubating
public final class OracleRelationalMetadataFactory implements RelationalMetadataFactory {

    public static final OracleRelationalMetadataFactory INSTANCE = new OracleRelationalMetadataFactory();

    private OracleRelationalMetadataFactory() {
    }

    @Override
    public OracleColumnEditor columnEditor() {
        return new OracleColumnEditorImpl();
    }

    @Override
    public OracleTableEditor tableEditor() {
        return new OracleTableEditorImpl();
    }

    @Override
    public OracleJsonTableChangeSerializer jsonTableChangesSerializer() {
        return new OracleJsonTableChangeSerializer(this);
    }
}
