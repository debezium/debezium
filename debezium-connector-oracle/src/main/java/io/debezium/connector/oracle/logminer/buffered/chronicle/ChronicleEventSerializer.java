/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.chronicle;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntry;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntryImpl;
import io.debezium.relational.TableId;

import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.ValueOut;

/**
 * Chronicle Queue serializer for LogMinerEvent objects.
 *
 * @author Debezium Authors
 */
public class ChronicleEventSerializer {
    private static final Logger LOGGER = LoggerFactory.getLogger(ChronicleEventSerializer.class);

    // Type identifiers for efficient serialization
    public static final byte TYPE_DML_EVENT = 1;
    public static final byte TYPE_SELECT_LOB_LOCATOR = 2;
    public static final byte TYPE_EXTENDED_STRING_BEGIN = 3;
    public static final byte TYPE_XML_BEGIN = 4;
    public static final byte TYPE_XML_WRITE = 5;
    public static final byte TYPE_XML_END = 6;
    public static final byte TYPE_LOB_WRITE = 7;
    public static final byte TYPE_TRUNCATE = 8;
    public static final byte TYPE_REDO_SQL_DML = 9;
    public static final byte TYPE_GENERIC_LOG_MINER_EVENT = 10;
    public static final byte TYPE_LOB_ERASE = 11;
    public static final byte TYPE_EXTENDED_STRING_WRITE = 12;

    // Value type markers
    static final byte VALUE_TYPE_NULL = 0;
    static final byte VALUE_TYPE_STRING = 1;
    static final byte VALUE_TYPE_INTEGER = 2;
    static final byte VALUE_TYPE_LONG = 3;
    static final byte VALUE_TYPE_DOUBLE = 4;
    static final byte VALUE_TYPE_FLOAT = 5;
    static final byte VALUE_TYPE_BOOLEAN = 6;
    static final byte VALUE_TYPE_BIG_DECIMAL = 7;
    static final byte VALUE_TYPE_BYTES = 8;
    static final byte VALUE_TYPE_UNAVAILABLE = 9;

    // Sentinel values
    private static final String UNAVAILABLE_VALUE_SENTINEL = "$$DBZ-UNAVAILABLE-VALUE$$";
    // Named sentinel constants for readability (public so serializers can reuse them)
    public static final long NULL_SCN = -1L;
    public static final long NULL_CHANGE_TIME = Long.MIN_VALUE;
    // Marker used when serializing arrays to indicate 'null' array
    public static final int ARRAY_NULL_COUNT = -1;

    /**
     * Convert Instant to epoch microseconds for storage.
     */
    public static long instantToMicros(Instant instant) {
        return (instant != null) ? instant.toEpochMilli() * 1000 : NULL_CHANGE_TIME;
    }

    /**
     * Convert stored epoch microseconds back to Instant.
     */
    public static Instant microsToInstant(long micros) {
        return (micros == NULL_CHANGE_TIME) ? null : Instant.ofEpochMilli(micros / 1000);
    }

    /**
     * Writes a LogMinerEvent to Chronicle Queue storage.
     *
     * @param appender the Chronicle Queue appender
     * @param eventKey the event key within the transaction
     * @param event the LogMinerEvent to write
     * @return the sequence index of the written document
     */
    public static long writeEvent(ExcerptAppender appender, int eventKey, LogMinerEvent event) {
        try (DocumentContext context = appender.writingDocument()) {
            ValueOut valueOut = context.wire().getValueOut();

            valueOut.int32(eventKey);

            EventSerializerStrategy serializer = ChronicleEventSerializerRegistry.getSerializer(event.getClass());

            valueOut.int8(serializer.getTypeId());

            serializer.writeEvent(valueOut, event);

            return context.index();
        }
        catch (Exception e) {
            throw new DebeziumException("Failed to write event " + eventKey + " to Chronicle Queue", e);
        }
    }

    /**
     * Reads a LogMinerEvent from Chronicle Queue storage.
     *
     * @param tailer the Chronicle Queue tailer positioned at the event
     * @return the reconstructed LogMinerEvent or null if no more events
     */
    public static LogMinerEvent readEvent(ExcerptTailer tailer) {
        try (DocumentContext context = tailer.readingDocument()) {
            if (!context.isPresent()) {
                return null;
            }

            ValueIn valueIn = context.wire().getValueIn();

            valueIn.int32();

            byte typeId = valueIn.int8();

            EventSerializerStrategy deserializer = ChronicleEventSerializerRegistry.getDeserializer(typeId);

            return deserializer.readEvent(valueIn);
        }
        catch (Exception e) {
            throw new DebeziumException("Failed to read event from Chronicle Queue", e);
        }
    }

    /**
     * Reads a LogMinerEvent and its eventKey from Chronicle Queue storage.
     *
     * @param tailer the Chronicle Queue tailer positioned at the event
     * @return a pair of eventKey and LogMinerEvent, or null if no more events
     */
    public static EventKeyAndEvent readEventWithKey(ExcerptTailer tailer) {
        try (DocumentContext context = tailer.readingDocument()) {
            if (!context.isPresent()) {
                return null;
            }

            ValueIn valueIn = context.wire().getValueIn();

            int eventKey = valueIn.int32();

            byte typeId = valueIn.int8();

            EventSerializerStrategy deserializer = ChronicleEventSerializerRegistry.getDeserializer(typeId);

            LogMinerEvent event = deserializer.readEvent(valueIn);
            return new EventKeyAndEvent(eventKey, event);
        }
        catch (Exception e) {
            throw new DebeziumException("Failed to read event with key from Chronicle Queue", e);
        }
    }

    // Strategy interface for event serialization
    public interface EventSerializerStrategy {
        byte getTypeId();

        void writeEvent(ValueOut valueOut, LogMinerEvent event);

        LogMinerEvent readEvent(ValueIn valueIn);
    }

    // Base serializer with common functionality
    public abstract static class BaseEventSerializer implements EventSerializerStrategy {

        protected void writeCommonFields(ValueOut valueOut, LogMinerEvent event) {
            valueOut.int8((byte) event.getEventType().ordinal());

            long scnValue = event.getScn().isNull() ? NULL_SCN : event.getScn().longValue();
            valueOut.int64(scnValue);

            long changeTimeMicros = instantToMicros(event.getChangeTime());
            valueOut.int64(changeTimeMicros);

            TableId tableId = event.getTableId();
            writeString(valueOut, tableId.catalog());
            writeString(valueOut, tableId.schema());
            writeString(valueOut, tableId.table());

            writeString(valueOut, event.getRowId());
            writeString(valueOut, event.getRsId());
        }

        protected LogMinerEvent readCommonFields(ValueIn valueIn, EventType eventType) {
            long scnValue = valueIn.int64();
            Scn scn = scnValue == NULL_SCN ? Scn.NULL : Scn.valueOf(scnValue);

            long changeTimeMicros = valueIn.int64();
            Instant changeTime = microsToInstant(changeTimeMicros);

            String catalog = readString(valueIn);
            String schema = readString(valueIn);
            String table = readString(valueIn);
            TableId tableId = new TableId(catalog, schema, table);

            String rowId = readString(valueIn);
            String rsId = readString(valueIn);

            return new LogMinerEvent(eventType, scn, tableId, rowId, rsId, changeTime);
        }

        protected void writeString(ValueOut valueOut, String str) {
            valueOut.text(str);
        }

        protected String readString(ValueIn valueIn) {
            return valueIn.text();
        }

        protected void writeObject(ValueOut valueOut, Object value) {
            if (value == null) {
                valueOut.int8(VALUE_TYPE_NULL);
            }
            else if (value == OracleValueConverters.UNAVAILABLE_VALUE) {
                valueOut.int8(VALUE_TYPE_UNAVAILABLE);
                writeString(valueOut, UNAVAILABLE_VALUE_SENTINEL);
            }
            else if (value instanceof String str) {
                valueOut.int8(VALUE_TYPE_STRING);
                writeString(valueOut, str);
            }
            else if (value instanceof Integer i) {
                valueOut.int8(VALUE_TYPE_INTEGER);
                valueOut.int32(i);
            }
            else if (value instanceof Long l) {
                valueOut.int8(VALUE_TYPE_LONG);
                valueOut.int64(l);
            }
            else if (value instanceof Double d) {
                valueOut.int8(VALUE_TYPE_DOUBLE);
                valueOut.float64(d);
            }
            else if (value instanceof Float f) {
                valueOut.int8(VALUE_TYPE_FLOAT);
                valueOut.float32(f);
            }
            else if (value instanceof Boolean b) {
                valueOut.int8(VALUE_TYPE_BOOLEAN);
                valueOut.bool(b);
            }
            else if (value instanceof BigDecimal bd) {
                valueOut.int8(VALUE_TYPE_BIG_DECIMAL);
                writeString(valueOut, bd.toString());
            }
            else if (value instanceof byte[] bytes) {
                valueOut.int8(VALUE_TYPE_BYTES);
                valueOut.int32(bytes.length);
                valueOut.bytes(bytes);
            }
            else if (value instanceof ByteBuffer buffer) {
                valueOut.int8(VALUE_TYPE_BYTES);
                valueOut.int32(buffer.remaining());
                byte[] bytes = new byte[buffer.remaining()];
                buffer.duplicate().get(bytes);
                valueOut.bytes(bytes);
            }
            else {
                LOGGER.debug("Unknown object type {} serializing as string: {}",
                        value.getClass().getName(), value);
                valueOut.int8(VALUE_TYPE_STRING);
                writeString(valueOut, value.toString());
            }
        }

        protected Object readObject(ValueIn valueIn) {
            byte typeMarker = valueIn.int8();
            switch (typeMarker) {
                case VALUE_TYPE_NULL:
                    return null;
                case VALUE_TYPE_UNAVAILABLE:
                    String stringValue = readString(valueIn);
                    if (UNAVAILABLE_VALUE_SENTINEL.equals(stringValue)) {
                        return OracleValueConverters.UNAVAILABLE_VALUE;
                    }
                    return stringValue;
                case VALUE_TYPE_STRING:
                    return readString(valueIn);
                case VALUE_TYPE_INTEGER:
                    return valueIn.int32();
                case VALUE_TYPE_LONG:
                    return valueIn.int64();
                case VALUE_TYPE_DOUBLE:
                    return valueIn.float64();
                case VALUE_TYPE_FLOAT:
                    return valueIn.float32();
                case VALUE_TYPE_BOOLEAN:
                    return valueIn.bool();
                case VALUE_TYPE_BIG_DECIMAL:
                    return new BigDecimal(readString(valueIn));
                case VALUE_TYPE_BYTES:
                    int length = valueIn.int32();
                    byte[] bytes = new byte[length];
                    valueIn.bytes(bytes);
                    return bytes;
                default:
                    throw new DebeziumException("Unknown value type marker: " + typeMarker);
            }
        }

        protected void writeValuesArray(ValueOut valueOut, Object[] values) {
            if (values != null) {
                valueOut.int32(values.length);
                for (Object value : values) {
                    writeObject(valueOut, value);
                }
            }
            else {
                valueOut.int32(ChronicleEventSerializer.ARRAY_NULL_COUNT);
            }
        }

        protected Object[] readValuesArray(ValueIn valueIn) {
            int count = valueIn.int32();
            if (count < 0) {
                return null;
            }

            Object[] values = new Object[count];
            for (int i = 0; i < count; i++) {
                values[i] = readObject(valueIn);
            }
            return values;
        }

        protected void writeDmlEntry(ValueOut valueOut, LogMinerDmlEntry dmlEntry) {
            writeValuesArray(valueOut, dmlEntry.getOldValues());
            writeValuesArray(valueOut, dmlEntry.getNewValues());
            writeString(valueOut, dmlEntry.getObjectOwner());
            writeString(valueOut, dmlEntry.getObjectName());
        }

        protected LogMinerDmlEntryImpl readDmlEntry(ValueIn valueIn, EventType eventType) {
            Object[] oldValues = readValuesArray(valueIn);
            Object[] newValues = readValuesArray(valueIn);
            String objectOwner = readString(valueIn);
            String objectName = readString(valueIn);
            return new LogMinerDmlEntryImpl(eventType.getValue(), newValues, oldValues, objectOwner, objectName);
        }
    }

    public record EventKeyAndEvent(int eventKey, LogMinerEvent event) {
    }

    public static class GenericLogMinerEventSerializer extends BaseEventSerializer {

        @Override
        public byte getTypeId() {
            return TYPE_GENERIC_LOG_MINER_EVENT;
        }

        @Override
        public void writeEvent(ValueOut valueOut, LogMinerEvent event) {
            writeCommonFields(valueOut, event);
        }

        @Override
        public LogMinerEvent readEvent(ValueIn valueIn) {
            byte eventTypeOrdinal = valueIn.int8();
            EventType[] eventTypes = EventType.values();
            if (eventTypeOrdinal < 0 || eventTypeOrdinal >= eventTypes.length) {
                throw new DebeziumException("Invalid EventType ordinal: " + eventTypeOrdinal +
                        ". Valid range is 0-" + (eventTypes.length - 1));
            }
            EventType eventType = eventTypes[eventTypeOrdinal];

            return readCommonFields(valueIn, eventType);
        }
    }
}
