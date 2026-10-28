/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.event;

import java.io.IOException;

import com.github.shyiko.mysql.binlog.event.deserialization.EventDataDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

/**
 * Reads the table id of a MySQL {@code PARTIAL_UPDATE_ROWS_EVENT}. The rows are not decoded, as the connector
 * can't capture this event; the binlog client skips the rest of the event body.
 */
public class PartialUpdateRowsEventDataDeserializer implements EventDataDeserializer<PartialUpdateRowsEventData> {

    @Override
    public PartialUpdateRowsEventData deserialize(ByteArrayInputStream inputStream) throws IOException {
        final var eventData = new PartialUpdateRowsEventData();
        eventData.setTableId(inputStream.readLong(6));
        return eventData;
    }
}
