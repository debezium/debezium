/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

public class ApproximateStructSizeCalculator {

    private static final int EMPTY_STRUCT_SIZE = 56;
    private static final int EMPTY_STRING_SIZE = 56;
    private static final int EMPTY_BYTES_SIZE = 24;
    private static final int EMPTY_ARRAY_SIZE = 64;
    private static final int EMPTY_MAP_SIZE = 88;
    private static final int EMPTY_PRIMITIVE = 24;
    private static final int REFERENCE_SIZE = 8;

    public static long getApproximateRecordSize(SourceRecord changeEvent) {
        // assuming 100 bytes per entry of partition / offset / header
        long value = changeEvent.sourcePartition().size() * 100 + changeEvent.sourceOffset().size() * 100 + changeEvent.headers().size() * 100;
        value += 8; // timestamp

        // key and value, ignoring schemas, assuming they are constant, shared on the heap
        return value + getStructSize((Struct) changeEvent.key()) + getStructSize((Struct) changeEvent.value())
                + changeEvent.topic().getBytes().length;
    }

    private static long getStructSize(Struct struct) {
        if (struct == null) {
            return 0;
        }
        long size = EMPTY_STRUCT_SIZE;
        final Schema schema = struct.schema();
        for (Field field : schema.fields()) {
            // every field requires a separate reference
            size += REFERENCE_SIZE;
            size += getValueSize(field.schema(), struct.getWithoutDefault(field.name()));
        }
        return size;
    }

    @SuppressWarnings("unchecked")
    private static long getValueSize(Schema schema, Object value) {
        switch (schema.type()) {
            case BOOLEAN:
            case INT8:
            case INT16:
            case FLOAT32:
            case INT32:
            case FLOAT64:
            case INT64:
                return EMPTY_PRIMITIVE;
            case STRING:
                final String s = (String) value;
                return (s == null) ? 0 : EMPTY_STRING_SIZE + s.getBytes().length;
            case BYTES:
                final byte[] b = (byte[]) value;
                return (b == null) ? 0 : EMPTY_BYTES_SIZE + b.length;
            case STRUCT:
                return getStructSize((Struct) value);
            case ARRAY:
                return getArraySize(schema.valueSchema(), (List<Object>) value);
            case MAP:
                return getMapSize(schema.keySchema(), schema.valueSchema(), (Map<Object, Object>) value);
        }
        return 0L;
    }

    private static long getArraySize(Schema elementSchema, List<Object> array) {
        if (array == null) {
            return 0L;
        }
        long size = EMPTY_ARRAY_SIZE;
        for (Object element : array) {
            size += REFERENCE_SIZE;
            size += getValueSize(elementSchema, element);
        }
        return size;
    }

    private static long getMapSize(Schema keySchema, Schema valueSchema, Map<Object, Object> map) {
        if (map == null) {
            return 0L;
        }
        long size = EMPTY_MAP_SIZE;
        for (Map.Entry<Object, Object> entry : map.entrySet()) {
            size += REFERENCE_SIZE * 2;
            size += getValueSize(keySchema, entry.getKey());
            size += getValueSize(valueSchema, entry.getValue());
        }
        return size;
    }
}