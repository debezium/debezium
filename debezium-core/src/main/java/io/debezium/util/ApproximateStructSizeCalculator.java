/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

public class ApproximateStructSizeCalculator {
    private static final int EMPTY_STRUCT_SIZE = 56;
    private static final int EMPTY_STRING_SIZE = 56;
    private static final int EMPTY_ARRAY_SIZE = 24;
    private static final int EMPTY_PRIMITIVE = 24;

    public static long getApproximateRecordSize(SourceRecord changeEvent) {
        // assuming 100 bytes per entry of partition / offset / header
        long value = changeEvent.sourcePartition().size() * 100 + changeEvent.sourceOffset().size() * 100 + changeEvent.headers().size() * 100;
        ;
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
            size += 8;
            switch (field.schema().type()) {
                case BOOLEAN:
                case INT8:
                case INT16:
                case FLOAT32:
                case INT32:
                case FLOAT64:
                case INT64:
                    size += EMPTY_PRIMITIVE;
                    break;
                case STRING:
                    final String s = (String) struct.getWithoutDefault(field.name());
                    size += (s == null) ? 0 : EMPTY_STRING_SIZE + s.getBytes().length;
                    break;
                case STRUCT:
                    size += getStructSize((Struct) struct.getWithoutDefault(field.name()));
                    break;
                case ARRAY:
                    break;
                case BYTES:
                    final byte[] b = (byte[]) struct.getWithoutDefault(field.name());
                    size += (b == null) ? 0 : EMPTY_ARRAY_SIZE + b.length;
                    break;
                case MAP:
                    break;
            }
        }
        return size;
    }
}
