/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.debezium.data.Envelope;

public class UppercaseUuidFieldsTest {

    private static final Schema SOURCE_SCHEMA = SchemaBuilder.struct()
            .field("schema", Schema.STRING_SCHEMA)
            .field("table", Schema.STRING_SCHEMA)
            .build();

    private static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
            .name("server1.dbo.Orders.Value")
            .field("id", Schema.INT64_SCHEMA)
            .field("order_guid", Schema.STRING_SCHEMA)
            .field("customer_guid", Schema.STRING_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .build();

    private final UppercaseUuidFields<SourceRecord> transform = new UppercaseUuidFields<>();

    @AfterEach
    public void tearDown() {
        transform.close();
    }

    @Test
    public void shouldUppercaseConfiguredUuidColumns() {
        transform.configure(Map.of("columns", "dbo.Orders.order_guid,dbo.Orders.customer_guid"));

        SourceRecord record = createRecord("dbo", "Orders",
                "6f9619ff-8b86-d011-b42d-00c04fc964ff",
                "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
                "some name");

        SourceRecord result = transform.apply(record);
        Struct after = ((Struct) result.value()).getStruct("after");

        assertThat(after.getString("order_guid")).isEqualTo("6F9619FF-8B86-D011-B42D-00C04FC964FF");
        assertThat(after.getString("customer_guid")).isEqualTo("A1B2C3D4-E5F6-7890-ABCD-EF1234567890");
        assertThat(after.getString("name")).isEqualTo("some name");
    }

    @Test
    public void shouldOnlyUppercaseSpecifiedColumns() {
        transform.configure(Map.of("columns", "dbo.Orders.order_guid"));

        SourceRecord record = createRecord("dbo", "Orders",
                "6f9619ff-8b86-d011-b42d-00c04fc964ff",
                "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
                "some name");

        SourceRecord result = transform.apply(record);
        Struct after = ((Struct) result.value()).getStruct("after");

        assertThat(after.getString("order_guid")).isEqualTo("6F9619FF-8B86-D011-B42D-00C04FC964FF");
        assertThat(after.getString("customer_guid")).isEqualTo("a1b2c3d4-e5f6-7890-abcd-ef1234567890");
    }

    @Test
    public void shouldNotAffectNonMatchingTables() {
        transform.configure(Map.of("columns", "dbo.Customers.order_guid"));

        SourceRecord record = createRecord("dbo", "Orders",
                "6f9619ff-8b86-d011-b42d-00c04fc964ff",
                "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
                "some name");

        SourceRecord result = transform.apply(record);
        Struct after = ((Struct) result.value()).getStruct("after");

        assertThat(after.getString("order_guid")).isEqualTo("6f9619ff-8b86-d011-b42d-00c04fc964ff");
    }

    @Test
    public void shouldHandleNullValues() {
        transform.configure(Map.of("columns", "dbo.Orders.order_guid"));

        Struct source = new Struct(SOURCE_SCHEMA)
                .put("schema", "dbo")
                .put("table", "Orders");

        Schema nullableValueSchema = SchemaBuilder.struct()
                .name("server1.dbo.Orders.Value")
                .field("id", Schema.INT64_SCHEMA)
                .field("order_guid", Schema.OPTIONAL_STRING_SCHEMA)
                .field("customer_guid", Schema.OPTIONAL_STRING_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .build();

        Struct row = new Struct(nullableValueSchema)
                .put("id", 1L)
                .put("order_guid", null)
                .put("customer_guid", "abc")
                .put("name", "test");

        Envelope envelope = Envelope.defineSchema()
                .withName("server1.dbo.Orders.Envelope")
                .withRecord(nullableValueSchema)
                .withSource(SOURCE_SCHEMA)
                .build();

        Struct payload = envelope.create(row, source, Instant.now());
        SourceRecord record = new SourceRecord(new HashMap<>(), new HashMap<>(), "topic", envelope.schema(), payload);

        SourceRecord result = transform.apply(record);
        Struct after = ((Struct) result.value()).getStruct("after");

        assertThat(after.get("order_guid")).isNull();
        assertThat(after.getString("customer_guid")).isEqualTo("abc");
    }

    @Test
    public void shouldHandleNullRecord() {
        transform.configure(Map.of("columns", "dbo.Orders.order_guid"));

        SourceRecord record = new SourceRecord(new HashMap<>(), new HashMap<>(), "topic", null, null);
        SourceRecord result = transform.apply(record);

        assertThat(result.value()).isNull();
    }

    @Test
    public void shouldHandleMultipleTables() {
        transform.configure(Map.of("columns", "dbo.Orders.order_guid,dbo.Customers.customer_id"));

        SourceRecord ordersRecord = createRecord("dbo", "Orders",
                "aabb-ccdd", "1234-5678", "order1");

        SourceRecord result = transform.apply(ordersRecord);
        Struct after = ((Struct) result.value()).getStruct("after");

        assertThat(after.getString("order_guid")).isEqualTo("AABB-CCDD");
        assertThat(after.getString("customer_guid")).isEqualTo("1234-5678");
    }

    @Test
    public void shouldBeCaseInsensitive() {
        // Config uses mixed case, source emits different case
        transform.configure(Map.of("columns", "DBO.Orders.Order_Guid"));

        SourceRecord record = createRecord("dbo", "orders",
                "6f9619ff-8b86-d011-b42d-00c04fc964ff",
                "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
                "some name");

        SourceRecord result = transform.apply(record);
        Struct after = ((Struct) result.value()).getStruct("after");

        assertThat(after.getString("order_guid")).isEqualTo("6F9619FF-8B86-D011-B42D-00C04FC964FF");
        assertThat(after.getString("customer_guid")).isEqualTo("a1b2c3d4-e5f6-7890-abcd-ef1234567890");
    }

    private SourceRecord createRecord(String schemaName, String tableName,
                                      String orderGuid, String customerGuid, String name) {
        Struct source = new Struct(SOURCE_SCHEMA)
                .put("schema", schemaName)
                .put("table", tableName);

        Struct row = new Struct(VALUE_SCHEMA)
                .put("id", 1L)
                .put("order_guid", orderGuid)
                .put("customer_guid", customerGuid)
                .put("name", name);

        Envelope envelope = Envelope.defineSchema()
                .withName("server1." + schemaName + "." + tableName + ".Envelope")
                .withRecord(VALUE_SCHEMA)
                .withSource(SOURCE_SCHEMA)
                .build();

        Struct payload = envelope.create(row, source, Instant.now());
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "topic", envelope.schema(), payload);
    }
}
