/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import io.debezium.connector.oracle.BaseOracleSchemaChangeEventEmitter;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerDdlEntry;
import io.debezium.pipeline.spi.SchemaChangeEventEmitter;
import io.debezium.relational.TableId;

/**
 * {@link SchemaChangeEventEmitter} implementation based on Oracle LogMiner utility.
 */
public class LogMinerSchemaChangeEventEmitter extends BaseOracleSchemaChangeEventEmitter {

    public LogMinerSchemaChangeEventEmitter(OracleOffsetContext offsetContext, TableId tableId, LogMinerDdlEntry ddlLcr) {
        super(offsetContext,
                tableId,
                tableId.catalog(), // todo tableId should be enough
                tableId.schema(), // todo same here
                ddlLcr.getDdlText(),
                ddlLcr.getCommandType());
    }
}
