/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.serde;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.json.JsonConverter;

import io.debezium.DebeziumException;
import io.debezium.annotation.ThreadSafe;
import io.debezium.common.annotation.Incubating;

/**
 * Serializes Kafka Connect field values, and {@link Struct} keys they may be stored under, to and from
 * bytes, preserving each value's exact runtime type. Useful for persistent caches or any store that must
 * later write a value back into an outgoing {@link Struct}.
 * <p>
 * Values are the final converted values as they appear in a Connect record's payload, and a restored
 * value is typically written back into an outgoing {@link Struct}, whose {@code put} validates the
 * value's exact runtime class against the field schema. The encoding therefore tags every value with a
 * type marker so deserialization restores the exact runtime type (e.g. an {@code Integer} never widens to
 * a {@code Long}, and a {@code byte[]} is distinguished from a {@link ByteBuffer}), rather than relying
 * on a schemaless representation that loses those distinctions.
 * <p>
 * The stored form is a versioned envelope: {@code [magic][formatVersion][writeTimestampMs][tagged value]}.
 * The magic bytes (ASCII {@code DBZ}) identify the bytes as Debezium-serialized; the write timestamp
 * supports read-side TTL enforcement by the caller; the version byte allows the format to evolve. Foreign
 * bytes and unknown versions surface as a {@link DebeziumException} that callers can treat as a missing
 * entry.
 * <p>
 * Values are stored without their schema (the tag byte alone restores the runtime type) with one
 * exception: {@link Struct} values (e.g. {@code VariableScaleDecimal}, geometry types) cannot be
 * reconstructed without their {@link org.apache.kafka.connect.data.Schema}, so they are delegated to
 * Kafka's {@link JsonConverter} with the schema embedded alongside the payload.
 * <p>
 * Null values keep their declared type: when the caller supplies the value's Connect {@link Schema}, a
 * null is written as the type's tag with the high bit set as a null flag, so the stored bytes record what
 * the value would have been (a null {@code INT32} is distinguishable from a null {@code STRING}). Without
 * a schema the type of a null is unknowable (null has no runtime class) and a plain untyped null tag is
 * written instead.
 * <p>
 * Unsupported value types raise a {@link DebeziumException} from {@link #serialize(Object, Schema, long)};
 * callers are expected to skip storing such values.
 * <p>
 * Keys use a separate, compare-only encoding produced by {@link #serializeStructIdentity(Struct)}: keys
 * are pure identity and are never deserialized, so nested structs are written positionally with no schema
 * detail, and a fingerprint of the key schema pins the shape (see that method for details).
 *
 * @author Chris Cranford
 */
@Incubating
@ThreadSafe
public final class ConnectValueSerde {

    // ASCII "DBZ": marks the bytes as Debezium-serialized, e.g. when inspecting a shared store.
    private static final byte[] MAGIC = { 'D', 'B', 'Z' };

    private static final byte FORMAT_VERSION = 1;

    private static final byte TAG_NULL = 0;
    private static final byte TAG_STRING = 1;
    private static final byte TAG_BOOLEAN = 2;
    private static final byte TAG_INT8 = 3;
    private static final byte TAG_INT16 = 4;
    private static final byte TAG_INT32 = 5;
    private static final byte TAG_INT64 = 6;
    private static final byte TAG_FLOAT32 = 7;
    private static final byte TAG_FLOAT64 = 8;
    private static final byte TAG_BYTE_ARRAY = 9;
    private static final byte TAG_BYTE_BUFFER = 10;
    private static final byte TAG_BIG_DECIMAL = 11;
    private static final byte TAG_BIG_INTEGER = 12;
    private static final byte TAG_DATE = 13;
    private static final byte TAG_STRUCT = 14;
    private static final byte TAG_LIST = 15;
    private static final byte TAG_MAP = 16;

    // High bit of the tag byte: the value is null, and the low seven bits carry the type it would have
    // had. TAG_NULL alone (no flag) is a null whose type was unknown at write time.
    private static final byte NULL_FLAG = (byte) 0x80;

    private static final int FINGERPRINT_LENGTH = 16;

    // Key value tags. Distinct from the value tags above: keys are compare-only identity bytes and are
    // never deserialized, so nested structs are written positionally without any schema detail.
    private static final byte KEY_TAG_NULL = 0;
    private static final byte KEY_TAG_STRING = 1;
    private static final byte KEY_TAG_BOOLEAN = 2;
    private static final byte KEY_TAG_INT8 = 3;
    private static final byte KEY_TAG_INT16 = 4;
    private static final byte KEY_TAG_INT32 = 5;
    private static final byte KEY_TAG_INT64 = 6;
    private static final byte KEY_TAG_FLOAT32 = 7;
    private static final byte KEY_TAG_FLOAT64 = 8;
    private static final byte KEY_TAG_BYTES = 9;
    private static final byte KEY_TAG_BIG_DECIMAL = 10;
    private static final byte KEY_TAG_BIG_INTEGER = 11;
    private static final byte KEY_TAG_DATE = 12;
    private static final byte KEY_TAG_STRUCT = 13;
    private static final byte KEY_TAG_LIST = 14;

    private final JsonConverter structConverter;

    public ConnectValueSerde() {
        this.structConverter = new JsonConverter();
        this.structConverter.configure(Map.of("schemas.enable", "true"), false);
    }

    /**
     * A deserialized cache entry: the value together with the wall-clock time it was written, for
     * read-side TTL enforcement.
     *
     * @param timestampMs the wall-clock time the value was serialized
     * @param value the deserialized value; may be null (a cached null)
     */
    public record DeserializedValue(long timestampMs, Object value) {
    }

    /**
     * Serialize the given value into the versioned, type-tagged envelope.
     * <p>
     * The schema only influences null handling: a non-null value is tagged by its runtime class, while a
     * null value is tagged with the type derived from the schema so the declared type survives
     * serialization. Container schemas ({@code ARRAY}, {@code MAP}) are descended into so nested nulls
     * are typed as well.
     *
     * @param value the value to serialize; may be null
     * @param schema the value's Connect schema, used to preserve the declared type of null values; may be
     *        null when unknown, in which case nulls are written untyped
     * @param timestampMs the wall-clock time of the write, stored for read-side TTL enforcement
     * @return the serialized bytes
     * @throws DebeziumException if the value's type is not supported by the encoding
     */
    public byte[] serialize(Object value, Schema schema, long timestampMs) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos)) {
            dos.write(MAGIC);
            dos.writeByte(FORMAT_VERSION);
            dos.writeLong(timestampMs);
            writeTaggedValue(dos, value, schema);
            dos.flush();
            return baos.toByteArray();
        }
        catch (IOException e) {
            throw new DebeziumException("Failed to serialize value", e);
        }
    }

    /**
     * Deserialize bytes previously produced by {@link #serialize(Object, Schema, long)}.
     *
     * @param bytes the serialized bytes; may not be null
     * @return the deserialized value and its write timestamp
     * @throws DebeziumException if the bytes are undecodable, lack the Debezium magic bytes, or use an
     *         unknown format version
     */
    public DeserializedValue deserialize(byte[] bytes) {
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes))) {
            final byte[] magic = new byte[MAGIC.length];
            dis.readFully(magic);
            if (!Arrays.equals(magic, MAGIC)) {
                throw new DebeziumException("Serialized value does not start with the Debezium magic bytes");
            }
            final byte version = dis.readByte();
            if (version != FORMAT_VERSION) {
                throw new DebeziumException("Unknown value format version: " + version);
            }
            final long timestampMs = dis.readLong();
            return new DeserializedValue(timestampMs, readTaggedValue(dis));
        }
        catch (IOException e) {
            throw new DebeziumException("Failed to deserialize value", e);
        }
    }

    /**
     * Serialize the given key {@link Struct} into stable, compare-only identity bytes.
     * <p>
     * The encoding is a truncated SHA-256 fingerprint over a canonical, recursive description of the key
     * schema (name, field names, field order and field types), followed by the key's field values written
     * positionally in schema order. Any change to the key schema changes the fingerprint and therefore
     * the identity bytes, so entries stored under the old shape naturally become unreachable. This is the
     * byte-level equivalent of {@code Struct} equality. Because the fingerprint pins the schema shape,
     * nested structs (e.g. {@code VariableScaleDecimal}) serialize positionally as their raw field values
     * with no schema detail, and the resulting bytes are never deserialized.
     *
     * @param key the key struct; may not be null
     * @return the identity bytes
     * @throws DebeziumException if the key contains a value whose type is not supported by the encoding
     */
    public byte[] serializeStructIdentity(Struct key) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos)) {
            dos.write(fingerprint(key.schema()));
            writeKeyStructFields(dos, key);
            dos.flush();
            return baos.toByteArray();
        }
        catch (IOException e) {
            throw new DebeziumException("Failed to serialize struct identity", e);
        }
    }

    private void writeTaggedValue(DataOutputStream dos, Object value, Schema schema) throws IOException {
        if (value == null) {
            dos.writeByte(schema != null ? (byte) (schemaTypeTag(schema) | NULL_FLAG) : TAG_NULL);
        }
        else if (value instanceof String string) {
            dos.writeByte(TAG_STRING);
            // Length-prefixed rather than writeUTF, whose 64KB limit is easily exceeded by LOB values.
            writeBytes(dos, string.getBytes(StandardCharsets.UTF_8));
        }
        else if (value instanceof Boolean bool) {
            dos.writeByte(TAG_BOOLEAN);
            dos.writeBoolean(bool);
        }
        else if (value instanceof Byte byteValue) {
            dos.writeByte(TAG_INT8);
            dos.writeByte(byteValue);
        }
        else if (value instanceof Short shortValue) {
            dos.writeByte(TAG_INT16);
            dos.writeShort(shortValue);
        }
        else if (value instanceof Integer intValue) {
            dos.writeByte(TAG_INT32);
            dos.writeInt(intValue);
        }
        else if (value instanceof Long longValue) {
            dos.writeByte(TAG_INT64);
            dos.writeLong(longValue);
        }
        else if (value instanceof Float floatValue) {
            dos.writeByte(TAG_FLOAT32);
            dos.writeFloat(floatValue);
        }
        else if (value instanceof Double doubleValue) {
            dos.writeByte(TAG_FLOAT64);
            dos.writeDouble(doubleValue);
        }
        else if (value instanceof byte[] byteArray) {
            dos.writeByte(TAG_BYTE_ARRAY);
            writeBytes(dos, byteArray);
        }
        else if (value instanceof ByteBuffer byteBuffer) {
            dos.writeByte(TAG_BYTE_BUFFER);
            writeBytes(dos, byteBufferContents(byteBuffer));
        }
        else if (value instanceof BigDecimal decimal) {
            dos.writeByte(TAG_BIG_DECIMAL);
            writeBigDecimal(dos, decimal);
        }
        else if (value instanceof BigInteger bigInteger) {
            dos.writeByte(TAG_BIG_INTEGER);
            writeBytes(dos, bigInteger.toByteArray());
        }
        else if (value.getClass() == Date.class) {
            // Exact class check: java.sql.Date/Time/Timestamp subclasses would not round-trip to their
            // original runtime class and are treated as unsupported instead.
            dos.writeByte(TAG_DATE);
            dos.writeLong(((Date) value).getTime());
        }
        else if (value instanceof Struct struct) {
            // A Struct cannot be reconstructed without its Schema, so embed the schema with the payload.
            dos.writeByte(TAG_STRUCT);
            writeBytes(dos, structConverter.fromConnectData(schemaTopic(struct), struct.schema(), struct));
        }
        else if (value instanceof List<?> list) {
            final Schema elementSchema = schema != null && schema.type() == Schema.Type.ARRAY ? schema.valueSchema() : null;
            dos.writeByte(TAG_LIST);
            dos.writeInt(list.size());
            for (Object element : list) {
                writeTaggedValue(dos, element, elementSchema);
            }
        }
        else if (value instanceof Map<?, ?> map) {
            final boolean mapSchema = schema != null && schema.type() == Schema.Type.MAP;
            final Schema keySchema = mapSchema ? schema.keySchema() : null;
            final Schema valueSchema = mapSchema ? schema.valueSchema() : null;
            dos.writeByte(TAG_MAP);
            dos.writeInt(map.size());
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                writeTaggedValue(dos, entry.getKey(), keySchema);
                writeTaggedValue(dos, entry.getValue(), valueSchema);
            }
        }
        else {
            throw new DebeziumException("Unsupported value type: " + value.getClass().getName());
        }
    }

    private Object readTaggedValue(DataInputStream dis) throws IOException {
        final byte tag = dis.readByte();
        if ((tag & NULL_FLAG) != 0) {
            // A typed null: the low bits record the declared type in the stored form, but the
            // deserialized value is simply null.
            return null;
        }
        switch (tag) {
            case TAG_NULL:
                return null;
            case TAG_STRING:
                return new String(readBytes(dis), StandardCharsets.UTF_8);
            case TAG_BOOLEAN:
                return dis.readBoolean();
            case TAG_INT8:
                return dis.readByte();
            case TAG_INT16:
                return dis.readShort();
            case TAG_INT32:
                return dis.readInt();
            case TAG_INT64:
                return dis.readLong();
            case TAG_FLOAT32:
                return dis.readFloat();
            case TAG_FLOAT64:
                return dis.readDouble();
            case TAG_BYTE_ARRAY:
                return readBytes(dis);
            case TAG_BYTE_BUFFER:
                return ByteBuffer.wrap(readBytes(dis));
            case TAG_BIG_DECIMAL:
                final int scale = dis.readInt();
                return new BigDecimal(new BigInteger(readBytes(dis)), scale);
            case TAG_BIG_INTEGER:
                return new BigInteger(readBytes(dis));
            case TAG_DATE:
                return new Date(dis.readLong());
            case TAG_STRUCT:
                final SchemaAndValue schemaAndValue = structConverter.toConnectData("", readBytes(dis));
                return schemaAndValue.value();
            case TAG_LIST:
                final int listSize = dis.readInt();
                final List<Object> list = new ArrayList<>(listSize);
                for (int i = 0; i < listSize; i++) {
                    list.add(readTaggedValue(dis));
                }
                return list;
            case TAG_MAP:
                final int mapSize = dis.readInt();
                final Map<Object, Object> map = new HashMap<>();
                for (int i = 0; i < mapSize; i++) {
                    final Object key = readTaggedValue(dis);
                    map.put(key, readTaggedValue(dis));
                }
                return map;
            default:
                throw new DebeziumException("Unknown value tag: " + tag);
        }
    }

    /**
     * Compute a truncated SHA-256 fingerprint over a canonical, recursive description of the key schema.
     */
    private static byte[] fingerprint(Schema schema) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos)) {
            describeSchema(dos, schema);
            dos.flush();
            final MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return Arrays.copyOf(digest.digest(baos.toByteArray()), FINGERPRINT_LENGTH);
        }
        catch (NoSuchAlgorithmException e) {
            throw new DebeziumException("SHA-256 is not available", e);
        }
    }

    private static void describeSchema(DataOutputStream dos, Schema schema) throws IOException {
        writeNullableString(dos, schema.name());
        dos.writeByte(schema.type().ordinal());
        dos.writeBoolean(schema.isOptional());
        final Integer version = schema.version();
        dos.writeInt(version != null ? version : 0);
        switch (schema.type()) {
            case STRUCT:
                dos.writeInt(schema.fields().size());
                for (org.apache.kafka.connect.data.Field field : schema.fields()) {
                    dos.writeUTF(field.name());
                    describeSchema(dos, field.schema());
                }
                break;
            case ARRAY:
                describeSchema(dos, schema.valueSchema());
                break;
            case MAP:
                describeSchema(dos, schema.keySchema());
                describeSchema(dos, schema.valueSchema());
                break;
            default:
                break;
        }
    }

    private static void writeKeyStructFields(DataOutputStream dos, Struct struct) throws IOException {
        // The fingerprint pins the schema shape, so field values are written positionally in schema order
        // with no per-field schema detail.
        for (org.apache.kafka.connect.data.Field field : struct.schema().fields()) {
            writeKeyValue(dos, struct.get(field));
        }
    }

    private static void writeKeyValue(DataOutputStream dos, Object value) throws IOException {
        if (value == null) {
            dos.writeByte(KEY_TAG_NULL);
        }
        else if (value instanceof String string) {
            dos.writeByte(KEY_TAG_STRING);
            writeBytes(dos, string.getBytes(StandardCharsets.UTF_8));
        }
        else if (value instanceof Boolean bool) {
            dos.writeByte(KEY_TAG_BOOLEAN);
            dos.writeBoolean(bool);
        }
        else if (value instanceof Byte byteValue) {
            dos.writeByte(KEY_TAG_INT8);
            dos.writeByte(byteValue);
        }
        else if (value instanceof Short shortValue) {
            dos.writeByte(KEY_TAG_INT16);
            dos.writeShort(shortValue);
        }
        else if (value instanceof Integer intValue) {
            dos.writeByte(KEY_TAG_INT32);
            dos.writeInt(intValue);
        }
        else if (value instanceof Long longValue) {
            dos.writeByte(KEY_TAG_INT64);
            dos.writeLong(longValue);
        }
        else if (value instanceof Float floatValue) {
            dos.writeByte(KEY_TAG_FLOAT32);
            dos.writeFloat(floatValue);
        }
        else if (value instanceof Double doubleValue) {
            dos.writeByte(KEY_TAG_FLOAT64);
            dos.writeDouble(doubleValue);
        }
        else if (value instanceof byte[] byteArray) {
            // byte[] and ByteBuffer share a tag: binary key identity is content-based.
            dos.writeByte(KEY_TAG_BYTES);
            writeBytes(dos, byteArray);
        }
        else if (value instanceof ByteBuffer byteBuffer) {
            dos.writeByte(KEY_TAG_BYTES);
            writeBytes(dos, byteBufferContents(byteBuffer));
        }
        else if (value instanceof BigDecimal decimal) {
            dos.writeByte(KEY_TAG_BIG_DECIMAL);
            writeBigDecimal(dos, decimal);
        }
        else if (value instanceof BigInteger bigInteger) {
            dos.writeByte(KEY_TAG_BIG_INTEGER);
            writeBytes(dos, bigInteger.toByteArray());
        }
        else if (value instanceof Date date) {
            dos.writeByte(KEY_TAG_DATE);
            dos.writeLong(date.getTime());
        }
        else if (value instanceof Struct struct) {
            dos.writeByte(KEY_TAG_STRUCT);
            writeKeyStructFields(dos, struct);
        }
        else if (value instanceof List<?> list) {
            dos.writeByte(KEY_TAG_LIST);
            dos.writeInt(list.size());
            for (Object element : list) {
                writeKeyValue(dos, element);
            }
        }
        else if (value instanceof Map<?, ?> map) {
            // Maps have no deterministic iteration order; entries are sorted by their encoded bytes so the
            // same logical map always produces the same key bytes.
            dos.writeByte(KEY_TAG_LIST);
            final List<byte[]> encodedEntries = new ArrayList<>(map.size());
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                try (ByteArrayOutputStream entryBaos = new ByteArrayOutputStream();
                        DataOutputStream entryDos = new DataOutputStream(entryBaos)) {
                    writeKeyValue(entryDos, entry.getKey());
                    writeKeyValue(entryDos, entry.getValue());
                    entryDos.flush();
                    encodedEntries.add(entryBaos.toByteArray());
                }
            }
            encodedEntries.sort(Arrays::compare);
            dos.writeInt(encodedEntries.size());
            for (byte[] encodedEntry : encodedEntries) {
                dos.write(encodedEntry);
            }
        }
        else {
            throw new DebeziumException("Unsupported key value type: " + value.getClass().getName());
        }
    }

    /**
     * Map a Connect schema to the tag its values would carry, used to type null values. Logical types
     * whose runtime class differs from their storage type are matched by name first; {@code BYTES} maps
     * to the byte-array tag since a null carries no runtime {@code byte[]}/{@link ByteBuffer} distinction.
     */
    private static byte schemaTypeTag(Schema schema) {
        final String schemaName = schema.name();
        if (Decimal.LOGICAL_NAME.equals(schemaName)) {
            return TAG_BIG_DECIMAL;
        }
        if (org.apache.kafka.connect.data.Date.LOGICAL_NAME.equals(schemaName)
                || Time.LOGICAL_NAME.equals(schemaName)
                || Timestamp.LOGICAL_NAME.equals(schemaName)) {
            return TAG_DATE;
        }
        switch (schema.type()) {
            case STRING:
                return TAG_STRING;
            case BOOLEAN:
                return TAG_BOOLEAN;
            case INT8:
                return TAG_INT8;
            case INT16:
                return TAG_INT16;
            case INT32:
                return TAG_INT32;
            case INT64:
                return TAG_INT64;
            case FLOAT32:
                return TAG_FLOAT32;
            case FLOAT64:
                return TAG_FLOAT64;
            case BYTES:
                return TAG_BYTE_ARRAY;
            case STRUCT:
                return TAG_STRUCT;
            case ARRAY:
                return TAG_LIST;
            case MAP:
                return TAG_MAP;
            default:
                // Serializing a null must never fail; fall back to an untyped null.
                return TAG_NULL;
        }
    }

    private static String schemaTopic(Struct struct) {
        return struct.schema().name() != null ? struct.schema().name() : "";
    }

    private static byte[] byteBufferContents(ByteBuffer byteBuffer) {
        final ByteBuffer duplicate = byteBuffer.duplicate();
        final byte[] contents = new byte[duplicate.remaining()];
        duplicate.get(contents);
        return contents;
    }

    private static void writeBigDecimal(DataOutputStream dos, BigDecimal decimal) throws IOException {
        dos.writeInt(decimal.scale());
        writeBytes(dos, decimal.unscaledValue().toByteArray());
    }

    private static void writeNullableString(DataOutputStream dos, String value) throws IOException {
        if (value == null) {
            dos.writeBoolean(false);
        }
        else {
            dos.writeBoolean(true);
            dos.writeUTF(value);
        }
    }

    private static void writeBytes(DataOutputStream dos, byte[] bytes) throws IOException {
        dos.writeInt(bytes.length);
        dos.write(bytes);
    }

    private static byte[] readBytes(DataInputStream dis) throws IOException {
        final byte[] bytes = new byte[dis.readInt()];
        dis.readFully(bytes);
        return bytes;
    }
}