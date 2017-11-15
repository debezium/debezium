/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.data;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * An immutable descriptor for the structure of Debezium message envelopes. An {@link Envelope} can be created for each message
 * schema using the {@link #defineSchema()} builder, and once created can generate {@link Struct} objects representing CREATE,
 * READ, UPDATE, and DELETE messages that conform to that schema.
 * 
 * @author Randall Hauch
 */
public final class Envelope {

    /**
     * The constants for the values for the {@link FieldName#OPERATION operation} field in the message envelope.
     */
    public static enum Operation {
        /**
         * The operation that read the current state of a record, most typically during snapshots.
         */
        READ("r"),
        /**
         * An operation that resulted in a new record being created in the source.
         */
        CREATE("c"),
        /**
         * An operation that resulted in an existing record being updated in the source.
         */
        UPDATE("u"),
        /**
         * An operation that resulted in an existing record being removed from or deleted in the source.
         */
        DELETE("d");
        private final String code;

        private Operation(String code) {
            this.code = code;
        }

        public static Operation forCode(String code) {
            for (Operation op : Operation.values()) {
                if (op.code().equalsIgnoreCase(code)) return op;
            }
            return null;
        }

        public String code() {
            return code;
        }
    }

    /**
     * The constants for the names of the fields in the message envelope.
     */
    public static final class FieldName {
        /**
         * The {@code before} field is used to store the state of a record before an operation.
         */
        public static final String BEFORE = "before";
        /**
         * The {@code after} field is used to store the state of a record after an operation.
         */
        public static final String AFTER = "after";
        /**
         * The {@code op} field is used to store the kind of operation on a record.
         */
        public static final String OPERATION = "op";
        /**
         * The {@code origin} field is used to store the information about the source of a record, including the
         * Kafka Connect partition and offset information.
         */
        public static final String SOURCE = "source";
        /**
         * The {@code ts_ms} field is used to store the information about the local time at which the connector
         * processed/generated the event. The timestamp values are the number of milliseconds past epoch (January 1, 1970), and
         * determined by the {@link System#currentTimeMillis() JVM current time in milliseconds}. Note that the <em>accuracy</em>
         * of the timestamp value depends on the JVM's system clock and all of its assumptions, limitations, conditions, and
         * variations.
         */
        public static final String TIMESTAMP = "ts_ms";
    }

    /**
     * Flag that specifies whether the {@link FieldName#OPERATION} field is required within the envelope.
     */
    public static final boolean OPERATION_REQUIRED = true;

    /**
     * The immutable set of all {@link FieldName}s.
     */
    public static final Set<String> ALL_FIELD_NAMES;

    static {
        Set<String> fields = new HashSet<>();
        fields.add(FieldName.OPERATION);
        fields.add(FieldName.TIMESTAMP);
        fields.add(FieldName.BEFORE);
        fields.add(FieldName.AFTER);
        fields.add(FieldName.SOURCE);
        ALL_FIELD_NAMES = Collections.unmodifiableSet(fields);
    }

    /**
     * A builder of an envelope schema.
     */
    public static interface Builder {
        /**
         * Define the {@link Schema} used in the {@link FieldName#BEFORE} and {@link FieldName#AFTER} fields.
         * 
         * @param schema the schema of the records, used in the {@link FieldName#BEFORE} and {@link FieldName#AFTER} fields; may
         *            not be null
         * @return this builder so methods can be chained; never null
         */
        default Builder withRecord(Schema schema) {
            return withSchema(schema, FieldName.BEFORE, FieldName.AFTER);
        }

        /**
         * Define the {@link Schema} used in the {@link FieldName#SOURCE} field.
         * 
         * @param sourceSchema the schema of the {@link FieldName#SOURCE} field; may not be null
         * @return this builder so methods can be chained; never null
         */
        default Builder withSource(Schema sourceSchema) {
            return withSchema(sourceSchema, FieldName.SOURCE);
        }

        /**
         * Define the {@link Schema} used for an arbitrary field in the envelope.
         * 
         * @param fieldNames the names of the fields that this schema should be used with; may not be null
         * @param fieldSchema the schema of the new optional field; may not be null
         * @return this builder so methods can be chained; never null
         */
        Builder withSchema(Schema fieldSchema, String... fieldNames);

        /**
         * Define the name for the schema.
         * 
         * @param name the name
         * @return this builder so methods can be chained; never null
         */
        Builder withName(String name);

        /**
         * Define the documentation for the schema.
         * 
         * @param doc the documentation
         * @return this builder so methods can be chained; never null
         */
        Builder withDoc(String doc);

        /**
         * Create the message envelope descriptor.
         * 
         * @return the envelope schema; never null
         */
        Envelope build();
    }

    public static Builder defineSchema() {
        return new Builder() {
            private SchemaBuilder builder = SchemaBuilder.struct();
            private Set<String> missingFields = new HashSet<>();

            @Override
            public Builder withSchema(Schema fieldSchema, String... fieldNames) {
                for (String fieldName : fieldNames) {
                    builder.field(fieldName, fieldSchema);
                }
                return this;
            }

            @Override
            public Builder withName(String name) {
                builder.name(name);
                return this;
            }

            @Override
            public Builder withDoc(String doc) {
                builder.doc(doc);
                return this;
            }

            @Override
            public Envelope build() {
                builder.field(FieldName.OPERATION, OPERATION_REQUIRED ? Schema.STRING_SCHEMA : Schema.OPTIONAL_STRING_SCHEMA);
                builder.field(FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA);
                checkFieldIsDefined(FieldName.OPERATION, OPERATION_REQUIRED);
                checkFieldIsDefined(FieldName.BEFORE, false);
                checkFieldIsDefined(FieldName.AFTER, false);
                checkFieldIsDefined(FieldName.SOURCE, false);
                if (!missingFields.isEmpty()) {
                    throw new IllegalStateException("The envelope schema is missing field(s) " + String.join(", ", missingFields));
                }
                return new Envelope(builder.build());
            }

            private void checkFieldIsDefined(String fieldName, boolean required) {
                if (builder.field(fieldName) == null) missingFields.add(fieldName);
            }
        };
    }

    private final Schema schema;

    private Envelope(Schema schema) {
        this.schema = schema;
    }

    /**
     * Get the {@link Schema} describing the message envelopes and their content.
     * 
     * @return the schema; never null
     */
    public Schema schema() {
        return schema;
    }

    /**
     * Generate a {@link Operation#READ read} message with the given information.
     * 
     * @param record the state of the record as read; may not be null
     * @param source the information about the source that was read; may be null
     * @param timestamp the timestamp for this message; may be null
     * @return the read message; never null
     */
    public Struct read(Object record, Struct source, Long timestamp) {
        Struct struct = new Struct(schema);
        struct.put(FieldName.OPERATION, Operation.READ.code());
        struct.put(FieldName.AFTER, record);
        if (source != null) struct.put(FieldName.SOURCE, source);
        if (timestamp != null) struct.put(FieldName.TIMESTAMP, timestamp);
        return struct;
    }

    /**
     * Generate a {@link Operation#CREATE read} message with the given information.
     * 
     * @param record the state of the record after creation; may not be null
     * @param source the information about the source where the creation occurred; may be null
     * @param timestamp the timestamp for this message; may be null
     * @return the create message; never null
     */
    public Struct create(Object record, Struct source, Long timestamp) {
        Struct struct = new Struct(schema);
        struct.put(FieldName.OPERATION, Operation.CREATE.code());
        struct.put(FieldName.AFTER, record);
        if (source != null) struct.put(FieldName.SOURCE, source);
        if (timestamp != null) struct.put(FieldName.TIMESTAMP, timestamp);
        return struct;
    }

    /**
     * Generate an {@link Operation#UPDATE update} message with the given information.
     * 
     * @param before the state of the record before the update; may be null
     * @param after the state of the record after the update; may not be null
     * @param source the information about the source where the update occurred; may be null
     * @param timestamp the timestamp for this message; may be null
     * @return the update message; never null
     */
    public Struct update(Object before, Struct after, Struct source, Long timestamp) {
        Struct struct = new Struct(schema);
        struct.put(FieldName.OPERATION, Operation.UPDATE.code());
        if (before != null) struct.put(FieldName.BEFORE, before);
        struct.put(FieldName.AFTER, after);
        if (source != null) struct.put(FieldName.SOURCE, source);
        if (timestamp != null) struct.put(FieldName.TIMESTAMP, timestamp);
        return struct;
    }

    /**
     * Generate an {@link Operation#DELETE delete} message with the given information.
     * 
     * @param before the state of the record before the delete; may be null
     * @param source the information about the source where the deletion occurred; may be null
     * @param timestamp the timestamp for this message; may be null
     * @return the delete message; never null
     */
    public Struct delete(Object before, Struct source, Long timestamp) {
        Struct struct = new Struct(schema);
        struct.put(FieldName.OPERATION, Operation.DELETE.code());
        if (before != null) struct.put(FieldName.BEFORE, before);
        if (source != null) struct.put(FieldName.SOURCE, source);
        if (timestamp != null) struct.put(FieldName.TIMESTAMP, timestamp);
        return struct;
    }

    /**
     * Obtain the operation for the given source record.
     * 
     * @param record the source record; may not be null
     * @return the operation, or null if no valid operation was found in the record
     */
    public static Operation operationFor(SourceRecord record) {
        Struct value = (Struct) record.value();
        Field opField = value.schema().field(FieldName.OPERATION);
        if (opField != null) {
            return Operation.forCode(value.getString(opField.name()));
        }
        return null;
    }
}
