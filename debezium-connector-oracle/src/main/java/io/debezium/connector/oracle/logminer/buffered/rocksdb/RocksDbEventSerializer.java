/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.rocksdb;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.relational.TableId;

/**
 * RocksDB serializer for LogMinerEvent objects.
 * @author Debezium Authors
 */
public class RocksDbEventSerializer {
    private static final Logger LOGGER = LoggerFactory.getLogger(RocksDbEventSerializer.class);

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

    private static final String UNAVAILABLE_VALUE_SENTINEL = "$$DBZ-UNAVAILABLE-VALUE$$";
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
     * Writes a LogMinerEvent to RocksDB storage.
     *
     * @param out the output stream
     * @param eventKey the event key within the transaction
     * @param event the LogMinerEvent to write
     * @throws IOException if an I/O error occurs
     */
    public static void writeEvent(DataOutputStream out, int eventKey, LogMinerEvent event) throws IOException {
        out.writeInt(eventKey);

        EventSerializerStrategy serializer = RocksDbEventSerializerRegistry.getSerializer(event.getClass());

        out.writeByte(serializer.getTypeId());

        serializer.writeEvent(out, event);
    }

    /**
     * Reads a LogMinerEvent from RocksDB storage.
     *
     * @param in the input stream
     * @return the reconstructed LogMinerEvent
     * @throws IOException if an I/O error occurs
     */
    public static LogMinerEvent readEvent(DataInputStream in) throws IOException {
        in.readInt(); // eventKey - consumed but not returned

        byte typeId = in.readByte();

        EventSerializerStrategy deserializer = RocksDbEventSerializerRegistry.getDeserializer(typeId);

        return deserializer.readEvent(in);
    }

    /**
     * Serializes a LogMinerEvent to a byte array.
     *
     * @param event the LogMinerEvent to serialize
     * @return the serialized byte array
     * @throws DebeziumException if serialization fails
     */
    public static byte[] serialize(LogMinerEvent event) {
        try (java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
                java.io.DataOutputStream dos = new java.io.DataOutputStream(baos)) {
            writeEvent(dos, 0, event); // eventKey is not used in this context
            return baos.toByteArray();
        }
        catch (IOException e) {
            throw new DebeziumException("Failed to serialize event", e);
        }
    }

    /**
     * Deserializes a LogMinerEvent from a byte array.
     *
     * @param data the byte array to deserialize
     * @return the deserialized LogMinerEvent
     * @throws DebeziumException if deserialization fails
     */
    public static LogMinerEvent deserialize(byte[] data) {
        try (java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(data);
                java.io.DataInputStream dis = new java.io.DataInputStream(bais)) {
            return readEvent(dis);
        }
        catch (IOException e) {
            throw new DebeziumException("Failed to deserialize event", e);
        }
    }

    // Strategy interface for event serialization
    public interface EventSerializerStrategy {
        /**
         * @return The unique byte identifier for the event type.
         */
        byte getTypeId();

        /**
         * Writes the event-specific fields to the output stream.
         *
         * @param out the output stream
         * @param event the event to write
         * @throws IOException if an I/O error occurs
         */
        void writeEvent(DataOutputStream out, LogMinerEvent event) throws IOException;

        /**
         * Reads the event-specific fields from the input stream.
         *
         * @param in the input stream
         * @return the reconstructed LogMinerEvent
         * @throws IOException if an I/O error occurs
         */
        LogMinerEvent readEvent(DataInputStream in) throws IOException;
    }

    // Base serializer with common functionality
    public abstract static class BaseEventSerializer implements EventSerializerStrategy {

        protected void writeCommonFields(DataOutputStream out, LogMinerEvent event) throws IOException {
            out.writeByte((byte) event.getEventType().ordinal());

            long scnValue = event.getScn().isNull() ? NULL_SCN : event.getScn().longValue();
            out.writeLong(scnValue);

            long changeTimeMicros = instantToMicros(event.getChangeTime());
            out.writeLong(changeTimeMicros);

            TableId tableId = event.getTableId();
            writeString(out, tableId.catalog());
            writeString(out, tableId.schema());
            writeString(out, tableId.table());

            writeString(out, event.getRowId());
            writeString(out, event.getRsId());
        }

        protected LogMinerEvent readCommonFields(DataInputStream in, EventType eventType) throws IOException {
            long scnValue = in.readLong();
            Scn scn = scnValue == NULL_SCN ? Scn.NULL : Scn.valueOf(scnValue);

            long changeTimeMicros = in.readLong();
            Instant changeTime = microsToInstant(changeTimeMicros);

            String catalog = readString(in);
            String schema = readString(in);
            String table = readString(in);
            TableId tableId = new TableId(catalog, schema, table);

            String rowId = readString(in);
            String rsId = readString(in);

            return new LogMinerEvent(eventType, scn, tableId, rowId, rsId, changeTime);
        }

        protected void writeString(DataOutputStream out, String str) throws IOException {
            if (str == null) {
                out.writeBoolean(true); // null flag = true (consistent with ehcache)
            }
            else {
                out.writeBoolean(false); // null flag = false (consistent with ehcache)
                // Use custom UTF-8 encoding for large strings to avoid writeUTF 64KB limit
                byte[] utf8Bytes = str.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                out.writeInt(utf8Bytes.length);
                out.write(utf8Bytes);
            }
        }

        protected String readString(DataInputStream in) throws IOException {
            boolean isNull = in.readBoolean();
            if (isNull) {
                return null;
            }
            // Read custom UTF-8 encoding
            int length = in.readInt();
            byte[] utf8Bytes = new byte[length];
            in.readFully(utf8Bytes);
            return new String(utf8Bytes, java.nio.charset.StandardCharsets.UTF_8);
        }

        protected void writeObject(DataOutputStream out, Object value) throws IOException {
            if (value == null) {
                out.writeByte(VALUE_TYPE_NULL);
            }
            else if (value == OracleValueConverters.UNAVAILABLE_VALUE) {
                out.writeByte(VALUE_TYPE_UNAVAILABLE);
                writeString(out, UNAVAILABLE_VALUE_SENTINEL);
            }
            else if (value instanceof String) {
                out.writeByte(VALUE_TYPE_STRING);
                writeString(out, (String) value);
            }
            else if (value instanceof Integer) {
                out.writeByte(VALUE_TYPE_INTEGER);
                out.writeInt((Integer) value);
            }
            else if (value instanceof Long) {
                out.writeByte(VALUE_TYPE_LONG);
                out.writeLong((Long) value);
            }
            else if (value instanceof Double) {
                out.writeByte(VALUE_TYPE_DOUBLE);
                out.writeDouble((Double) value);
            }
            else if (value instanceof Float) {
                out.writeByte(VALUE_TYPE_FLOAT);
                out.writeFloat((Float) value);
            }
            else if (value instanceof Boolean) {
                out.writeByte(VALUE_TYPE_BOOLEAN);
                out.writeBoolean((Boolean) value);
            }
            else if (value instanceof java.math.BigDecimal) {
                out.writeByte(VALUE_TYPE_BIG_DECIMAL);
                writeString(out, value.toString());
            }
            else if (value instanceof byte[]) {
                out.writeByte(VALUE_TYPE_BYTES);
                byte[] bytes = (byte[]) value;
                out.writeInt(bytes.length);
                out.write(bytes);
            }
            else if (value instanceof java.nio.ByteBuffer) {
                java.nio.ByteBuffer buffer = (java.nio.ByteBuffer) value;
                out.writeByte(VALUE_TYPE_BYTES);
                out.writeInt(buffer.remaining());
                byte[] bytes = new byte[buffer.remaining()];
                buffer.duplicate().get(bytes);
                out.write(bytes);
            }
            else {
                LOGGER.debug("Unknown object type {} serializing as string: {}",
                        value.getClass().getName(), value);
                out.writeByte(VALUE_TYPE_STRING);
                writeString(out, value.toString());
            }
        }

        protected Object readObject(DataInputStream in) throws IOException {
            byte typeMarker = in.readByte();
            switch (typeMarker) {
                case VALUE_TYPE_NULL:
                    return null;
                case VALUE_TYPE_UNAVAILABLE:
                    String stringValue = readString(in);
                    if (UNAVAILABLE_VALUE_SENTINEL.equals(stringValue)) {
                        return OracleValueConverters.UNAVAILABLE_VALUE;
                    }
                    return stringValue;
                case VALUE_TYPE_STRING:
                    return readString(in);
                case VALUE_TYPE_INTEGER:
                    return in.readInt();
                case VALUE_TYPE_LONG:
                    return in.readLong();
                case VALUE_TYPE_DOUBLE:
                    return in.readDouble();
                case VALUE_TYPE_FLOAT:
                    return in.readFloat();
                case VALUE_TYPE_BOOLEAN:
                    return in.readBoolean();
                case VALUE_TYPE_BIG_DECIMAL:
                    return new java.math.BigDecimal(readString(in));
                case VALUE_TYPE_BYTES:
                    int length = in.readInt();
                    byte[] bytes = new byte[length];
                    in.readFully(bytes);
                    return bytes;
                default:
                    throw new DebeziumException("Unknown value type marker: " + typeMarker);
            }
        }

        /**
         * Writes an array of values using the protected writeObject helper. Uses -1 to mark null arrays.
         */
        protected void writeValuesArray(DataOutputStream out, Object[] values) throws IOException {
            if (values == null) {
                out.writeInt(ARRAY_NULL_COUNT);
                return;
            }
            out.writeInt(values.length);
            for (Object value : values) {
                writeObject(out, value);
            }
        }

        /**
         * Reads an array of values using the protected readObject helper. Returns null when -1 marker is read.
         */
        protected Object[] readValuesArray(DataInputStream in) throws IOException {
            int length = in.readInt();
            if (length == ARRAY_NULL_COUNT) {
                return null;
            }
            Object[] values = new Object[length];
            for (int i = 0; i < length; i++) {
                values[i] = readObject(in);
            }
            return values;
        }
    }

    // Record to hold eventKey and event together
    public record EventKeyAndEvent(int eventKey, LogMinerEvent event) {
    }

    // Generic serializer for base LogMinerEvent class
    public static class GenericLogMinerEventSerializer extends BaseEventSerializer {

        @Override
        public byte getTypeId() {
            return TYPE_GENERIC_LOG_MINER_EVENT;
        }

        @Override
        public void writeEvent(DataOutputStream out, LogMinerEvent event) throws IOException {
            // Write common fields only - no additional DML-specific data
            writeCommonFields(out, event);
        }

        @Override
        public LogMinerEvent readEvent(DataInputStream in) throws IOException {
            // Read event type
            byte eventTypeOrdinal = in.readByte();
            EventType eventType = EventType.values()[eventTypeOrdinal];

            // Read common fields
            return readCommonFields(in, eventType);
        }
    }
}