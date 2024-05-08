/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.event;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Map;

import com.github.luben.zstd.Zstd;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.TransactionPayloadEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.TransactionPayloadEventDataDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import io.debezium.config.CommonConnectorConfig;

/**
 * @author Chris Cranford
 */
public class TransactionPayloadDeserializer extends TransactionPayloadEventDataDeserializer {

    private final Map<Long, TableMapEventData> tableMapEventByTableId;
    private final CommonConnectorConfig.EventProcessingFailureHandlingMode eventDeserializationFailureHandlingMode;

    public TransactionPayloadDeserializer(Map<Long, TableMapEventData> tableMapEventByTableId,
                                          CommonConnectorConfig.EventProcessingFailureHandlingMode eventDeserializationFailureHandlingMode) {
        this.tableMapEventByTableId = tableMapEventByTableId;
        this.eventDeserializationFailureHandlingMode = eventDeserializationFailureHandlingMode;
    }

    @Override
    public TransactionPayloadEventData deserialize(ByteArrayInputStream inputStream) throws IOException {
        TransactionPayloadEventData eventData = new TransactionPayloadEventData();
        // Read the header fields from the event data
        while (inputStream.available() > 0) {
            int fieldType = 0;
            int fieldLen = 0;
            // Read the type of the field
            if (inputStream.available() >= 1) {
                fieldType = inputStream.readPackedInteger();
            }
            // We have reached the end of the Event Data Header
            if (fieldType == OTW_PAYLOAD_HEADER_END_MARK) {
                break;
            }
            // Read the size of the field
            if (inputStream.available() >= 1) {
                fieldLen = inputStream.readPackedInteger();
            }
            switch (fieldType) {
                case OTW_PAYLOAD_SIZE_FIELD:
                    // Fetch the payload size
                    eventData.setPayloadSize(inputStream.readPackedInteger());
                    break;
                case OTW_PAYLOAD_COMPRESSION_TYPE_FIELD:
                    // Fetch the compression type
                    eventData.setCompressionType(inputStream.readPackedInteger());
                    break;
                case OTW_PAYLOAD_UNCOMPRESSED_SIZE_FIELD:
                    // Fetch the uncompressed size
                    eventData.setUncompressedSize(inputStream.readPackedInteger());
                    break;
                default:
                    // Ignore unrecognized field
                    inputStream.read(fieldLen);
                    break;
            }
        }
        if (eventData.getUncompressedSize() == 0) {
            // Default the uncompressed to the payload size
            eventData.setUncompressedSize(eventData.getPayloadSize());
        }
        // set the payload to the rest of the input buffer
        eventData.setPayload(inputStream.read(eventData.getPayloadSize()));

        // Decompress the payload
        byte[] src = eventData.getPayload();
        byte[] dst = ByteBuffer.allocate(eventData.getUncompressedSize()).array();
        Zstd.decompressByteArray(dst, 0, dst.length, src, 0, src.length);

        // Read and store events from decompressed byte array into input stream
        ArrayList<Event> decompressedEvents = new ArrayList<>();
        EventDeserializer transactionPayloadEventDeserializer = new EventDeserializer();
        transactionPayloadEventDeserializer.setEventDataDeserializer(EventType.WRITE_ROWS,
                new RowDeserializers.WriteRowsDeserializer(tableMapEventByTableId, eventDeserializationFailureHandlingMode));
        transactionPayloadEventDeserializer.setEventDataDeserializer(EventType.UPDATE_ROWS,
                new RowDeserializers.UpdateRowsDeserializer(tableMapEventByTableId, eventDeserializationFailureHandlingMode));
        transactionPayloadEventDeserializer.setEventDataDeserializer(EventType.DELETE_ROWS,
                new RowDeserializers.DeleteRowsDeserializer(tableMapEventByTableId, eventDeserializationFailureHandlingMode));
        transactionPayloadEventDeserializer.setEventDataDeserializer(EventType.EXT_WRITE_ROWS,
                new RowDeserializers.WriteRowsDeserializer(
                        tableMapEventByTableId, eventDeserializationFailureHandlingMode).setMayContainExtraInformation(true));
        transactionPayloadEventDeserializer.setEventDataDeserializer(EventType.EXT_UPDATE_ROWS,
                new RowDeserializers.UpdateRowsDeserializer(
                        tableMapEventByTableId, eventDeserializationFailureHandlingMode).setMayContainExtraInformation(true));
        transactionPayloadEventDeserializer.setEventDataDeserializer(EventType.EXT_DELETE_ROWS,
                new RowDeserializers.DeleteRowsDeserializer(
                        tableMapEventByTableId, eventDeserializationFailureHandlingMode).setMayContainExtraInformation(true));

        ByteArrayInputStream destinationInputStream = new ByteArrayInputStream(dst);

        Event internalEvent = transactionPayloadEventDeserializer.nextEvent(destinationInputStream);
        while (internalEvent != null) {
            decompressedEvents.add(internalEvent);
            if (internalEvent.getHeader().getEventType() == EventType.TABLE_MAP && internalEvent.getData() != null) {
                TableMapEventData tableMapEvent = internalEvent.getData();
                tableMapEventByTableId.put(tableMapEvent.getTableId(), tableMapEvent);
            }

            internalEvent = transactionPayloadEventDeserializer.nextEvent(destinationInputStream);
        }

        eventData.setUncompressedEvents(decompressedEvents);

        return eventData;
    }
}
