/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.event;

import com.github.shyiko.mysql.binlog.event.EventData;

/**
 * Event data of a MySQL {@code PARTIAL_UPDATE_ROWS_EVENT}, which the server writes instead of an
 * {@code UPDATE_ROWS_EVENT} when {@code binlog_row_value_options=PARTIAL_JSON}. Only the table id is read.
 *
 * @see PartialUpdateRowsEventDataDeserializer
 */
public class PartialUpdateRowsEventData implements EventData {

    private long tableId;

    public long getTableId() {
        return tableId;
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    @Override
    public String toString() {
        return "PartialUpdateRowsEventData{tableId=" + tableId + "}";
    }
}
