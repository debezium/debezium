/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.xstream;

import io.debezium.connector.oracle.BaseOracleSchemaChangeEventEmitter;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.pipeline.spi.SchemaChangeEventEmitter;
import io.debezium.relational.TableId;

import oracle.streams.DDLLCR;

/**
 * {@link SchemaChangeEventEmitter} implementation based on Oracle.
 *
 * @author Gunnar Morling
 */
public class XStreamSchemaChangeEventEmitter extends BaseOracleSchemaChangeEventEmitter {

    public XStreamSchemaChangeEventEmitter(OracleOffsetContext offsetContext, TableId tableId, DDLLCR ddlLcr) {
        super(offsetContext,
                tableId,
                ddlLcr.getSourceDatabaseName(),
                ddlLcr.getObjectOwner(),
                ddlLcr.getDDLText(),
                ddlLcr.getCommandType());
    }
}
