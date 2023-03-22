/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.util;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import io.debezium.data.Envelope;
import io.debezium.util.Strings;

/**
 * Helper class for easily building {@link SinkRecord} instances using a builder pattern.
 *
 * @author Chris Cranford
 */
public class SinkRecordBuilder {

    private SinkRecordBuilder() {
    }

    public static SinkRecordTypeBuilder create() {
        return new SinkRecordTypeBuilder(Type.CREATE);
    }

    public static SinkRecordTypeBuilder update() {
        return new SinkRecordTypeBuilder(Type.UPDATE);
    }

    public static SinkRecordTypeBuilder delete() {
        return new SinkRecordTypeBuilder(Type.DELETE);
    }

    public static SinkRecordTypeBuilder tombstone() {
        return new SinkRecordTypeBuilder(Type.TOMBSTONE);
    }

    public static class SinkRecordTypeBuilder {
        private final Type type;

        private boolean flat;
        private String topicName;
        private String name;
        private Schema keySchema;
        private Schema recordSchema;
        private Schema sourceSchema;
        private int partition;
        private int offset;
        private Map<String, Object> keyValues = new HashMap<>();
        private Map<String, Object> beforeValues = new HashMap<>();
        private Map<String, Object> afterValues = new HashMap<>();
        private Map<String, Object> sourceValues = new HashMap<>();

        private SinkRecordTypeBuilder(Type type) {
            this.type = type;
        }

        public SinkRecordTypeBuilder flat(boolean flat) {
            this.flat = flat;
            return this;
        }

        public SinkRecordTypeBuilder topic(String topicName) {
            this.topicName = topicName;
            return this;
        }

        public SinkRecordTypeBuilder name(String name) {
            this.name = name;
            return this;
        }

        public SinkRecordTypeBuilder keySchema(Schema keySchema) {
            this.keySchema = keySchema;
            return this;
        }

        public SinkRecordTypeBuilder key(String fieldName, Object value) {
            keyValues.put(fieldName, value);
            return this;
        }

        public SinkRecordTypeBuilder recordSchema(Schema recordSchema) {
            this.recordSchema = recordSchema;
            return this;
        }

        public SinkRecordTypeBuilder before(String fieldName, Object value) {
            beforeValues.put(fieldName, value);
            return this;
        }

        public SinkRecordTypeBuilder after(String fieldName, Object value) {
            afterValues.put(fieldName, value);
            return this;
        }

        public SinkRecordTypeBuilder sourceSchema(Schema sourceSchema) {
            this.sourceSchema = sourceSchema;
            return this;
        }

        public SinkRecordTypeBuilder source(String fieldName, Object value) {
            sourceValues.put(fieldName, value);
            return this;
        }

        public SinkRecordTypeBuilder partition(int partition) {
            this.partition = partition;
            return this;
        }

        public SinkRecordTypeBuilder offset(int offset) {
            this.offset = offset;
            return this;
        }

        public SinkRecord build() {
            switch (type) {
                case CREATE:
                    return buildCreateSinkRecord();
                case UPDATE:
                    return buildUpdateSinkRecord();
                case DELETE:
                    return buildDeleteSinkRecord();
                case TOMBSTONE:
                    return buildTombstoneSinkRecord();
            }
            return null;
        }

        private SinkRecord buildCreateSinkRecord() {
            Objects.requireNonNull(recordSchema, "A record schema must be provided.");
            Objects.requireNonNull(sourceSchema, "A source schema must be provided.");

            final Struct key = populateStructForKey();
            final Struct after = populateStructFromMap(new Struct(recordSchema), afterValues);
            final Struct source = populateStructFromMap(new Struct(sourceSchema), sourceValues);

            if (!flat) {
                final Envelope envelope = createEnvelope();
                final Struct payload = envelope.create(after, source, Instant.now());
                return new SinkRecord(topicName, partition, keySchema, key, envelope.schema(), payload, offset);
            }
            else {
                return new SinkRecord(topicName, partition, keySchema, key, recordSchema, after, offset);
            }
        }

        private SinkRecord buildUpdateSinkRecord() {
            Objects.requireNonNull(recordSchema, "A record schema must be provided.");
            Objects.requireNonNull(sourceSchema, "A source schema must be provided.");

            final Struct key = populateStructForKey();
            final Struct before = populateStructFromMap(new Struct(recordSchema), beforeValues);
            final Struct after = populateStructFromMap(new Struct(recordSchema), afterValues);
            final Struct source = populateStructFromMap(new Struct(sourceSchema), sourceValues);

            if (!flat) {
                final Envelope envelope = createEnvelope();
                final Struct payload = envelope.update(before, after, source, Instant.now());
                return new SinkRecord(topicName, partition, keySchema, key, envelope.schema(), payload, offset);
            }
            else {
                return new SinkRecord(topicName, partition, keySchema, key, recordSchema, after, offset);
            }
        }

        private SinkRecord buildDeleteSinkRecord() {
            Objects.requireNonNull(recordSchema, "A record schema must be provided.");
            Objects.requireNonNull(sourceSchema, "A source schema must be provided.");

            final Struct key = populateStructForKey();
            final Struct before = populateStructFromMap(new Struct(recordSchema), beforeValues);
            final Struct source = populateStructFromMap(new Struct(sourceSchema), sourceValues);

            if (!flat) {
                final Envelope envelope = createEnvelope();
                final Struct payload = envelope.delete(before, source, Instant.now());
                return new SinkRecord(topicName, partition, keySchema, key, envelope.schema(), payload, offset);
            }
            else {
                return new SinkRecord(topicName, partition, keySchema, key, recordSchema, null, offset);
            }
        }

        private SinkRecord buildTombstoneSinkRecord() {
            final Struct key = populateStructForKey();
            return new SinkRecord(topicName, partition, keySchema, key, null, null, offset);
        }

        private Envelope createEnvelope() {
            return Envelope.defineSchema()
                    .withRecord(recordSchema)
                    .withSource(sourceSchema)
                    .withName((Strings.isNullOrBlank(name) ? "dummy" : name) + ".Envelope")
                    .build();
        }

        private Struct populateStructFromMap(Struct struct, Map<String, Object> values) {
            for (Map.Entry<String, Object> entry : values.entrySet()) {
                struct.put(entry.getKey(), entry.getValue());
            }
            return struct;
        }

        private Struct populateStructForKey() {
            if (keySchema != null) {
                return populateStructFromMap(new Struct(keySchema), keyValues);
            }
            return null;
        }
    }

    private enum Type {
        CREATE,
        UPDATE,
        DELETE,
        TOMBSTONE
    }
}
