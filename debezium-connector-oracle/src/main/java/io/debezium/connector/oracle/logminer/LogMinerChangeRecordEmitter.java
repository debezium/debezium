/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.util.Arrays;
import java.util.List;

import io.debezium.connector.oracle.BaseChangeRecordEmitter;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerColumnValue;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerDmlEntry;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.Table;
import io.debezium.util.Clock;

/**
 * Emits change record based on a single {@link LogMinerDmlEntry} event.
 */
public class LogMinerChangeRecordEmitter extends BaseChangeRecordEmitter<LogMinerColumnValue> {

    private LogMinerDmlEntry dmlEntry;
    protected final Table table;

    public LogMinerChangeRecordEmitter(OffsetContext offset, LogMinerDmlEntry dmlEntry, Table table, Clock clock) {
        super(offset, table, clock);
        this.dmlEntry = dmlEntry;
        this.table = table;
    }

    @Override
    protected Operation getOperation() {
        return dmlEntry.getCommandType();
    }

    @Override
    protected Object[] getOldColumnValues() {
        List<LogMinerColumnValue> valueList = dmlEntry.getOldValues();
        LogMinerColumnValue[] result = Arrays.copyOf(valueList.toArray(), valueList.size(), LogMinerColumnValue[].class);
        return getColumnValues(result);
    }

    @Override
    protected Object[] getNewColumnValues() {
        List<LogMinerColumnValue> valueList = dmlEntry.getNewValues();
        LogMinerColumnValue[] result = Arrays.copyOf(valueList.toArray(), valueList.size(), LogMinerColumnValue[].class);
        return getColumnValues(result);
    }

    @Override
    protected String getColumnName(LogMinerColumnValue columnValue) {
        return columnValue.getColumnName();
    }

    protected Object getColumnData(LogMinerColumnValue columnValue) {
        return columnValue.getColumnData();
    }
}
