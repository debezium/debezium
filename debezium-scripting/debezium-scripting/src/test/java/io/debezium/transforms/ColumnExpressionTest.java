/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static io.debezium.transforms.TransformsUtils.createDeleteRecord;
import static io.debezium.transforms.TransformsUtils.createNullRecord;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.time.IsoTimestamp;
import io.debezium.transforms.ScriptingTransformation.NullHandling;

/**
 * @author Felix Dörre
 */
public class ColumnExpressionTest {

    public static final String TOPIC_REGEX = "topic.regex";
    public static final String LANGUAGE = "language";
    public static final String HEADER = "header";
    public static final String EXPRESSION = "expression";
    public static final String NULL_HANDLING = "null.handling.mode";

    @Test
    void testLanguageRequired() {
        assertThrows(DebeziumException.class, () -> {
            try (ColumnExpression<SourceRecord> transform = new ColumnExpression<>()) {
                final Map<String, String> props = new HashMap<>();
                props.put(EXPRESSION, "operation != 'd'");
                transform.configure(props);
            }
        });
    }

    @Test
    void testExpressionRequired() {
        assertThrows(DebeziumException.class, () -> {
            try (ColumnExpression<SourceRecord> transform = new ColumnExpression<>()) {
                final Map<String, String> props = new HashMap<>();
                props.put(LANGUAGE, "jsr223.groovy");
                transform.configure(props);
            }
        });
    }

    @Test
    void shouldFailOnUnkownLanguage() {
        assertThrows(DebeziumException.class, () -> {
            try (ColumnExpression<SourceRecord> transform = new ColumnExpression<>()) {
                final Map<String, String> props = new HashMap<>();
                props.put(EXPRESSION, "operation != 'd'");
                props.put(LANGUAGE, "jsr223.jython");
                transform.configure(props);
            }
        });
    }

    @Test
    void shouldFailToParseCondition() {
        assertThrows(DebeziumException.class, () -> {
            try (ColumnExpression<SourceRecord> transform = new ColumnExpression<>()) {
                final Map<String, String> props = new HashMap<>();
                props.put(EXPRESSION, "operation != 'd");
                props.put(LANGUAGE, "jsr223.groovy");
                transform.configure(props);
            }
        });
    }

    @Test
    void shouldProcessExpression() {
        try (ColumnExpression<SourceRecord> transform = new ColumnExpression<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, "return [name: row.name+\"-transformed\"]");
            props.put(LANGUAGE, "jsr223.groovy");
            transform.configure(props);
            final SourceRecord record = createDeleteRecord(1);
            assertEquals(((Struct) record.value()).getStruct("before").getString("name") + "-transformed",
                    ((Struct) transform.apply(record).value()).getStruct("before").getString("name"));
        }
    }

    @Test
    void shouldProcessFullRecord() {
        try (ColumnExpression<SourceRecord> transform = new ColumnExpression<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HEADER,
                    "import org.apache.kafka.connect.data.Field;\n" + "import org.apache.kafka.connect.data.Schema;\n"
                            + "import org.apache.kafka.connect.data.SchemaBuilder;\n"
                            + "import org.apache.kafka.connect.data.Struct;\n"
                            + "import org.apache.kafka.connect.source.SourceRecord;\n"
                            + "import org.apache.kafka.connect.transforms.util.SchemaUtil;\n"
                            + "@groovy.transform.CompileStatic");
            props.put(EXPRESSION,
                    """
                            SchemaBuilder valueSchema = io.debezium.data.SchemaUtil.copySchema(record.valueSchema()).field("myConstant",
                                    SchemaBuilder.string().build());
                            Struct result = new Struct(valueSchema);
                            Struct valueOld = ((Struct) record.value());
                            for (Field f : valueOld.schema().fields()) {
                                if (valueOld.get(f)!=null) {
                                    result.put(f, valueOld.get(f));
                                }
                            }
                            result.put("myConstant", "42");
                            record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), valueSchema, result, record.timestamp());
                            """);
            props.put(LANGUAGE, "jsr223.groovy");
            transform.configure(props);
            final SourceRecord record = createDeleteRecord(1);
            System.out.println(record);

            assertEquals(((Struct) record.value()).getStruct("before").getString("name"),
                    ((Struct) transform.apply(record).value()).getStruct("before").getString("name"));
            assertEquals("42", ((Struct) transform.apply(record).value()).getString("myConstant"));
        }
    }

    @Test
    void shouldProcessFullSourceInfo() {
        try (ColumnExpression<SourceRecord> transform = new ColumnExpression<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HEADER,
                    "import org.apache.kafka.connect.data.Field;\n" + "import org.apache.kafka.connect.data.Schema;\n"
                            + "import org.apache.kafka.connect.data.SchemaBuilder;\n"
                            + "import org.apache.kafka.connect.data.Struct;\n"
                            + "import org.apache.kafka.connect.source.SourceRecord;\n"
                            + "import org.apache.kafka.connect.transforms.util.SchemaUtil;\n"
                            + "import io.debezium.time.IsoTimestamp;\n" + "import java.sql.Timestamp;\n"
                            + "op_map = [\"d\":\"D\", \"c\":\"I\", \"r\":\"I\", \"u\":\"U\"]\n");
            props.put(EXPRESSION,
                    """
                            SchemaBuilder valueSchema = io.debezium.data.SchemaUtil.copySchema(record.valueSchema())
                                .field("op_type",SchemaBuilder.string().build())
                                .field("op_ts",SchemaBuilder.string().build())
                                .field("pos",SchemaBuilder.string().build());
                            Struct result = new Struct(valueSchema);
                            Struct valueOld = ((Struct) record.value());
                            for (Field f : valueOld.schema().fields()) {
                                if (valueOld.get(f)!=null) {
                                    result.put(f, valueOld.get(f));
                                }
                            }
                            result.put("op_ts", IsoTimestamp.toIsoString(new Timestamp(valueOld.getInt64("ts_ms")), null));
                            result.put("op_type", op_map[valueOld.getString("op")]);
                            result.put("pos", Integer.toHexString(valueOld.getStruct("source").getInt32("lsn")));
                            record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), valueSchema, result, record.timestamp());
                            """);
            props.put(LANGUAGE, "jsr223.groovy");
            transform.configure(props);
            final SourceRecord record = createDeleteRecord(1);

            SourceRecord transformed = transform.apply(record);

            System.out.println(record);
            System.out.println(transformed);

            assertEquals(IsoTimestamp.toIsoString(new Timestamp(((Struct) record.value()).getInt64("ts_ms")), null),
                    ((Struct) transformed.value()).getString("op_ts"));
            assertEquals("D", ((Struct) transformed.value()).getString("op_type"));
            assertEquals(Integer.toHexString(1234), ((Struct) transformed.value()).getString("pos"));
        }
    }

    @Test
    void shouldProcessTruncation() {
        try (ColumnExpression<SourceRecord> transform = new ColumnExpression<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HEADER, "@groovy.transform.CompileStatic");
            props.put(EXPRESSION,
                    "return [name: row.getString(\"name\").substring(0,Math.min(row.getString(\"name\").length(),4))]");
            props.put(LANGUAGE, "jsr223.groovy");
            transform.configure(props);
            final SourceRecord record = createDeleteRecord(1);
            assertEquals(((Struct) record.value()).getStruct("before").getString("name").substring(0, 4),
                    ((Struct) transform.apply(record).value()).getStruct("before").getString("name"));
        }
    }

    @Test
    void shouldRunJavaScript() {
        try (ColumnExpression<SourceRecord> transform = new ColumnExpression<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, "return {name: value.name+\"-transformed\"}");
            props.put(LANGUAGE, "jsr223.graal.js");
            transform.configure(props);
            final SourceRecord record = createDeleteRecord(1);
            assertEquals(((Struct) record.value()).getStruct("before").getString("name") + "-transformed",
                    ((Struct) transform.apply(record).value()).getStruct("before").getString("name"));
        }
    }

    @Test
    void shouldProcessFullJs() {
        try (ColumnExpression<SourceRecord> transform = new ColumnExpression<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HEADER,
                    "var SchemaBuilder = Java.type(\"org.apache.kafka.connect.data.SchemaBuilder\");\n"
                            + "var Struct = Java.type(\"org.apache.kafka.connect.data.Struct\");\n"
                            + "var SchemaUtil = Java.type(\"io.debezium.data.SchemaUtil\");");
            props.put(EXPRESSION,
                    """
                            let valueSchema = SchemaUtil.copySchema(record.valueSchema()).field("myConstant",
                                    SchemaBuilder.string().build());
                            let result = new Struct(valueSchema);
                            let valueOld = record.value();
                            for (let f of valueOld.schema().fields()) {
                                if (valueOld.get(f)!=null) {
                                    result.put(f, valueOld.get(f));
                                }
                            }
                            result.put("myConstant", "42");
                            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), valueSchema, result, record.timestamp());
                            """);
            props.put(LANGUAGE, "jsr223.graal.js");
            transform.configure(props);
            final SourceRecord record = createDeleteRecord(1);

            assertEquals(((Struct) record.value()).getStruct("before").getString("name"),
                    ((Struct) transform.apply(record).value()).getStruct("before").getString("name"));
            assertEquals("42", ((Struct) transform.apply(record).value()).getString("myConstant"));
        }
    }

    @Test
    void shouldProcessFullJsNull() {
        try (ColumnExpression<SourceRecord> transform = new ColumnExpression<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(ScriptingTransformation.NULL_HANDLING.name(), NullHandling.EVALUATE.toString());
            props.put(EXPRESSION,
                    """
                            return record.newRecord(record.topic().toUpperCase(), record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), record.value(), record.timestamp());
                            """);
            props.put(LANGUAGE, "jsr223.graal.js");
            transform.configure(props);

            final SourceRecord record = createNullRecord();
            final SourceRecord transformed = transform.apply(record);

            assertEquals(record.topic().toUpperCase(), transformed.topic());
            assertNull(transformed.value());
        }
    }
}
