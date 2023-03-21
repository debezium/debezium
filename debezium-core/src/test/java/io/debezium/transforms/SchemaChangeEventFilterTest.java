/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.transforms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import io.debezium.relational.history.ConnectTableChangeSerializer;
import io.debezium.relational.history.HistoryRecord;

/**
 * Unit test for the {@link SchemaChangeEventFilter} single message transformation.
 *
 */
public class SchemaChangeEventFilterTest extends AbstractExtractStateTest {

    private static final String SCHEMA_CHANGE_EVENT_INCLUDE_LIST = "schema.change.event.include.list";
    private static final String SCHEMA_HISTORY_CHANGE_SCHEMA_NAME = "io.debezium.connector.schema.Change";

    @Test
    public void whenNoDeclaredConfigExceptionIsThrew() {
        try (SchemaChangeEventFilter<SourceRecord> transform = new SchemaChangeEventFilter<>()) {
            final Map<String, String> props = new HashMap<>();
            assertThatThrownBy(() -> transform.configure(props)).isInstanceOf(ConfigException.class).hasMessageContaining(
                    "Invalid value null for configuration schema.change.event.include.list: The 'schema.change.event.include.list' value is invalid: A value is required");
        }
    }

    @Test
    public void testSchemaChangeContainsEventTypeFilter() {
        try (SchemaChangeEventFilter<SourceRecord> transform = new SchemaChangeEventFilter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(SCHEMA_CHANGE_EVENT_INCLUDE_LIST, "ALTER,CREATE");
            transform.configure(props);
            final SourceRecord record = createSchemaChangeRecordContainsEventType();
            assertThat(transform.apply(record));
        }
    }

    @Test
    public void testSchemaChangeNonEventTypeFilter() {
        try (SchemaChangeEventFilter<SourceRecord> transform = new SchemaChangeEventFilter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(SCHEMA_CHANGE_EVENT_INCLUDE_LIST, "ALTER,CREATE");
            transform.configure(props);
            final SourceRecord record = createSchemaChangeRecordNonEventType();
            assertThat(transform.apply(record)).isNull();
        }
    }

    @Test
    public void testSchemaChangeNonContainsEventTypeFilter() {
        try (SchemaChangeEventFilter<SourceRecord> transform = new SchemaChangeEventFilter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(SCHEMA_CHANGE_EVENT_INCLUDE_LIST, "ALTER,CREATE");
            transform.configure(props);
            final SourceRecord record = createSchemaChangeRecordNonContainsEventType();
            assertThat(transform.apply(record)).isNull();
        }
    }

    protected SourceRecord createSchemaChangeRecordNonEventType() {
        final Schema schemaChangeRecordSchema = createSchemaChangeSchema();
        final Struct result = new Struct(schemaChangeRecordSchema);
        result.put(HistoryRecord.Fields.TIMESTAMP, System.currentTimeMillis());
        result.put(HistoryRecord.Fields.DATABASE_NAME, "test");
        result.put(HistoryRecord.Fields.SCHEMA_NAME, "test_schema");
        result.put(HistoryRecord.Fields.DDL_STATEMENTS, "");
        result.put(HistoryRecord.Fields.TABLE_CHANGES, new ArrayList<>());
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", schemaChangeRecordSchema, result);
    }

    protected SourceRecord createSchemaChangeRecordContainsEventType() {
        final Schema schemaChangeRecordSchema = createSchemaChangeSchema();
        final Struct result = new Struct(schemaChangeRecordSchema);
        result.put(HistoryRecord.Fields.TIMESTAMP, System.currentTimeMillis());
        result.put(HistoryRecord.Fields.DATABASE_NAME, "test");
        result.put(HistoryRecord.Fields.SCHEMA_NAME, "test_schema");
        result.put(HistoryRecord.Fields.DDL_STATEMENTS, "");

        List<Struct> structs = new ArrayList<>();
        Struct struct = new Struct(schemaHistoryChangeSchema());
        struct.put(ConnectTableChangeSerializer.TYPE_KEY, "ALTER");
        struct.put(ConnectTableChangeSerializer.ID_KEY, "test.table");
        structs.add(struct);
        result.put(HistoryRecord.Fields.TABLE_CHANGES, structs);

        // value.put()
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", schemaChangeRecordSchema, result);
    }

    protected SourceRecord createSchemaChangeRecordNonContainsEventType() {
        final Schema schemaChangeRecordSchema = createSchemaChangeSchema();
        final Struct result = new Struct(schemaChangeRecordSchema);
        result.put(HistoryRecord.Fields.TIMESTAMP, System.currentTimeMillis());
        result.put(HistoryRecord.Fields.DATABASE_NAME, "test");
        result.put(HistoryRecord.Fields.SCHEMA_NAME, "test_schema");
        result.put(HistoryRecord.Fields.DDL_STATEMENTS, "");

        List<Struct> structs = new ArrayList<>();
        Struct struct = new Struct(schemaHistoryChangeSchema());
        struct.put(ConnectTableChangeSerializer.TYPE_KEY, "DROP");
        struct.put(ConnectTableChangeSerializer.ID_KEY, "test.table");
        structs.add(struct);
        result.put(HistoryRecord.Fields.TABLE_CHANGES, structs);

        result.put(HistoryRecord.Fields.TABLE_CHANGES, structs);

        // value.put()
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", schemaChangeRecordSchema, result);
    }

    private static Schema createSchemaChangeSchema() {
        final Schema schemaChangeRecordSchema = SchemaBuilder.struct()
                .name("filter.SchemaChangeValue")
                .field(HistoryRecord.Fields.TIMESTAMP, Schema.INT64_SCHEMA)
                .field(HistoryRecord.Fields.DATABASE_NAME, Schema.OPTIONAL_STRING_SCHEMA)
                .field(HistoryRecord.Fields.SCHEMA_NAME, Schema.OPTIONAL_STRING_SCHEMA)
                .field(HistoryRecord.Fields.DDL_STATEMENTS, Schema.OPTIONAL_STRING_SCHEMA)
                .field(HistoryRecord.Fields.TABLE_CHANGES, SchemaBuilder.array(schemaHistoryChangeSchema()).build())
                .build();
        return schemaChangeRecordSchema;
    }

    protected static Schema schemaHistoryChangeSchema() {
        return SchemaBuilder.struct()
                .name(SCHEMA_HISTORY_CHANGE_SCHEMA_NAME)
                .field(ConnectTableChangeSerializer.TYPE_KEY, Schema.STRING_SCHEMA)
                .field(ConnectTableChangeSerializer.ID_KEY, Schema.STRING_SCHEMA)
                .build();
    }

}
