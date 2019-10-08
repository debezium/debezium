/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.data;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * Utilities for obtaining JSON string representations of {@link Schema}, {@link Struct}, and {@link Field} objects.
 *
 * @author Randall Hauch
 */
public class SchemaUtil {

    private SchemaUtil() {
    }

    /**
     * Obtain a JSON string representation of the specified field.
     *
     * @param field the field; may not be null
     * @return the JSON string representation
     */
    public static String asString(Object field) {
        return new RecordWriter().append(field).toString();
    }

    /**
     * Obtain a JSON string representation of the specified field.
     *
     * @param field the field; may not be null
     * @return the JSON string representation
     */
    public static String asString(Field field) {
        return new RecordWriter().append(field).toString();
    }

    /**
     * Obtain a JSON string representation of the specified {@link Struct}.
     *
     * @param struct the {@link Struct}; may not be null
     * @return the JSON string representation
     */
    public static String asString(Struct struct) {
        return new RecordWriter().append(struct).toString();
    }

    /**
     * Obtain a JSON string representation of the specified {@link Schema}.
     *
     * @param schema the {@link Schema}; may not be null
     * @return the JSON string representation
     */
    public static String asString(Schema schema) {
        return new RecordWriter().append(schema).toString();
    }

    /**
     * Obtain a JSON string representation of the specified {@link SourceRecord}.
     *
     * @param record the {@link SourceRecord}; may not be null
     * @return the JSON string representation
     */
    public static String asString(SourceRecord record) {
        return new RecordWriter().append(record).toString();
    }

    /**
     * Obtain a JSON string representation of the specified field.
     *
     * @param field the field; may not be null
     * @return the JSON string representation
     */
    public static String asDetailedString(Field field) {
        return new RecordWriter().detailed(true).append(field).toString();
    }

    /**
     * Obtain a JSON string representation of the specified {@link Struct}.
     *
     * @param struct the {@link Struct}; may not be null
     * @return the JSON string representation
     */
    public static String asDetailedString(Struct struct) {
        return new RecordWriter().detailed(true).append(struct).toString();
    }

    /**
     * Obtain a JSON string representation of the specified {@link Schema}.
     *
     * @param schema the {@link Schema}; may not be null
     * @return the JSON string representation
     */
    public static String asDetailedString(Schema schema) {
        return new RecordWriter().detailed(true).append(schema).toString();
    }

    /**
     * Obtain a JSON string representation of the specified {@link SourceRecord}.
     *
     * @param record the {@link SourceRecord}; may not be null
     * @return the JSON string representation
     */
    public static String asDetailedString(SourceRecord record) {
        return new RecordWriter().detailed(true).append(record).toString();
    }

    private static class RecordWriter {
        private final StringBuilder sb = new StringBuilder();
        private boolean detailed = false;

        public RecordWriter detailed(boolean detailed) {
            this.detailed = detailed;
            return this;
        }

        @Override
        public String toString() {
            return sb.toString();
        }

        public RecordWriter append(Object obj) {
            if (obj == null) {
                sb.append("null");
            }
            else if (obj instanceof Schema) {
                Schema schema = (Schema) obj;
                sb.append('{');
                if (schema.name() != null) {
                    appendFirst("name", schema.name());
                    appendAdditional("type", schema.type());
                }
                else {
                    appendFirst("type", schema.type());
                }
                appendAdditional("optional", schema.isOptional());
                if (schema.doc() != null) {
                    appendAdditional("doc", schema.doc());
                }
                if (schema.version() != null) {
                    appendAdditional("version", schema.version());
                }
                switch (schema.type()) {
                    case STRUCT:
                        appendAdditional("fields", schema.fields());
                        break;
                    case MAP:
                        appendAdditional("key", schema.keySchema());
                        appendAdditional("value", schema.valueSchema());
                        break;
                    case ARRAY:
                        appendAdditional("value", schema.valueSchema());
                        break;
                    default:
                }
                sb.append('}');
            }
            else if (obj instanceof Struct) {
                Struct s = (Struct) obj;
                sb.append('{');
                boolean first = true;
                for (Field field : s.schema().fields()) {
                    if (first) {
                        first = false;
                    }
                    else {
                        sb.append(", ");
                    }
                    appendFirst(field.name(), s.get(field));
                }
                sb.append('}');
            }
            else if (obj instanceof ByteBuffer) {
                append((ByteBuffer) obj);
            }
            else if (obj instanceof byte[]) {
                append((byte[]) obj);
            }
            else if (obj instanceof Map<?, ?>) {
                Map<?, ?> map = (Map<?, ?>) obj;
                sb.append('{');
                boolean first = true;
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    if (first) {
                        appendFirst(entry.getKey().toString(), entry.getValue());
                        first = false;
                    }
                    else {
                        appendAdditional(entry.getKey().toString(), entry.getValue());
                    }
                }
                sb.append('}');
            }
            else if (obj instanceof List<?>) {
                List<?> list = (List<?>) obj;
                sb.append('[');
                boolean first = true;
                for (Object value : list) {
                    if (first) {
                        first = false;
                    }
                    else {
                        sb.append(", ");
                    }
                    append(value);
                }
                sb.append(']');
            }
            else if (obj instanceof Field) {
                Field f = (Field) obj;
                sb.append('{');
                appendFirst("name", f.name());
                appendAdditional("index", f.index());
                appendAdditional("schema", f.schema());
                sb.append('}');
            }
            else if (obj instanceof String) {
                sb.append('"').append(obj.toString()).append('"');
            }
            else if (obj instanceof Type) {
                sb.append('"').append(obj.toString()).append('"');
            }
            else if (obj instanceof SourceRecord) {
                SourceRecord record = (SourceRecord) obj;
                sb.append('{');
                appendFirst("sourcePartition", record.sourcePartition());
                appendAdditional("sourceOffset", record.sourceOffset());
                appendAdditional("topic", record.topic());
                appendAdditional("kafkaPartition", record.kafkaPartition());
                if (detailed) {
                    appendAdditional("keySchema", record.keySchema());
                }
                appendAdditional("key", record.key());
                if (detailed) {
                    appendAdditional("valueSchema", record.valueSchema());
                }
                appendAdditional("value", record.value());
                sb.append('}');
            }
            else {
                if (obj instanceof java.sql.Time) {
                    java.sql.Time time = (java.sql.Time) obj;
                    append(DateTimeFormatter.ISO_LOCAL_TIME.format(time.toLocalTime()));
                }
                else if (obj instanceof java.sql.Date) {
                    java.sql.Date date = (java.sql.Date) obj;
                    append(DateTimeFormatter.ISO_DATE.format(date.toLocalDate()));
                }
                else if (obj instanceof java.sql.Timestamp) {
                    java.sql.Timestamp ts = (java.sql.Timestamp) obj;
                    Instant instant = ts.toInstant();
                    append(DateTimeFormatter.ISO_INSTANT.format(instant));
                }
                else if (obj instanceof java.util.Date) {
                    java.util.Date date = (java.util.Date) obj;
                    append(DateTimeFormatter.ISO_INSTANT.format(date.toInstant()));
                }
                else if (obj instanceof TemporalAccessor) {
                    TemporalAccessor temporal = (TemporalAccessor) obj;
                    append(DateTimeFormatter.ISO_INSTANT.format(temporal));
                }
                else {
                    append(obj.toString());
                }
            }
            return this;
        }

        protected void append(ByteBuffer b) {
            append(b.array());
        }

        protected void append(byte[] b) {
            sb.append(Arrays.toString(b));
        }

        protected void appendFirst(String name, Object value) {
            append(name);
            sb.append(" : ");
            append(value);
        }

        protected void appendAdditional(String name, Object value) {
            sb.append(", ");
            append(name);
            sb.append(" : ");
            append(value);
        }
    }

}
