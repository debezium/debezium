/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.data;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;

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
    public static String asString(Field field) {
        StringBuilder sb = new StringBuilder();
        append(field, sb);
        return sb.toString();
    }

    /**
     * Obtain a JSON string representation of the specified {@link Struct}.
     * 
     * @param struct the {@link Struct}; may not be null
     * @return the JSON string representation
     */
    public static String asString(Struct struct) {
        StringBuilder sb = new StringBuilder();
        append(struct, sb);
        return sb.toString();
    }

    /**
     * Obtain a JSON string representation of the specified {@link Schema}.
     * 
     * @param schema the {@link Schema}; may not be null
     * @return the JSON string representation
     */
    public static String asString(Schema schema) {
        StringBuilder sb = new StringBuilder();
        append(schema, sb);
        return sb.toString();
    }

    protected static void append(Object obj, StringBuilder sb) {
        if (obj == null) {
            sb.append("null");
        } else if (obj instanceof Schema) {
            Schema schema = (Schema) obj;
            sb.append('{');
            if (schema.name() != null) {
                appendFirst(sb, "name", schema.name());
                appendAdditional(sb, "type", schema.type());
            } else {
                appendFirst(sb, "type", schema.type());
            }
            appendAdditional(sb, "optional", schema.isOptional());
            if (schema.doc() != null) appendAdditional(sb, "doc", schema.doc());
            if (schema.version() != null) appendAdditional(sb, "version", schema.version());
            switch (schema.type()) {
                case STRUCT:
                    appendAdditional(sb, "fields", schema.fields());
                    break;
                case MAP:
                    appendAdditional(sb, "key", schema.keySchema());
                    appendAdditional(sb, "value", schema.valueSchema());
                    break;
                case ARRAY:
                    appendAdditional(sb, "value", schema.valueSchema());
                    break;
                default:
            }
            sb.append('}');
        } else if (obj instanceof Struct) {
            Struct s = (Struct) obj;
            sb.append('{');
            boolean first = true;
            for (Field field : s.schema().fields()) {
                if (first)
                    first = false;
                else
                    sb.append(", ");
                appendFirst(sb, field.name(), s.get(field));
            }
            sb.append('}');
        } else if (obj instanceof Map<?, ?>) {
            Map<?, ?> map = (Map<?, ?>) obj;
            sb.append('{');
            boolean first = true;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (first)
                    first = false;
                else
                    sb.append(", ");
                sb.append("{ ");
                appendFirst(sb, "key", entry.getKey());
                appendAdditional(sb, "value", entry.getValue());
                sb.append(" } ");
            }
            sb.append('}');
        } else if (obj instanceof List<?>) {
            List<?> list = (List<?>) obj;
            sb.append('[');
            boolean first = true;
            for (Object value : list) {
                if (first)
                    first = false;
                else
                    sb.append(", ");
                append(value, sb);
            }
            sb.append(']');
        } else if (obj instanceof Field) {
            Field f = (Field) obj;
            sb.append('{');
            appendFirst(sb, "name", f.name());
            appendAdditional(sb, "index", f.index());
            appendAdditional(sb, "schema", f.schema());
            sb.append('}');
        } else if (obj instanceof String) {
            sb.append('"').append(obj.toString()).append('"');
        } else if (obj instanceof Type) {
            sb.append('"').append(obj.toString()).append('"');
        } else {
            sb.append(obj.toString());
        }
    }

    protected static void appendFirst(StringBuilder sb, String name, Object value) {
        append(name, sb);
        sb.append(" : ");
        append(value, sb);
    }

    protected static void appendAdditional(StringBuilder sb, String name, Object value) {
        sb.append(", ");
        append(name, sb);
        sb.append(" : ");
        append(value, sb);
    }

}
